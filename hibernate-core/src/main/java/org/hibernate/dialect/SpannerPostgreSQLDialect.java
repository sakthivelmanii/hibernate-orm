/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.dialect;

import jakarta.persistence.Timeout;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.hibernate.FetchMode;
import org.hibernate.JDBCException;
import org.hibernate.LockOptions;
import org.hibernate.MappingException;
import org.hibernate.Timeouts;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.model.FunctionContributions;
import org.hibernate.boot.model.TypeContributions;
import org.hibernate.boot.model.relational.SqlStringGenerationContext;
import org.hibernate.boot.spi.MetadataBuildingContext;
import org.hibernate.dialect.aggregate.AggregateSupport;
import org.hibernate.dialect.aggregate.SpannerPostgreSQLAggregateSupport;
import org.hibernate.dialect.function.CommonFunctionFactory;
import org.hibernate.dialect.function.PostgreSQLTruncRoundFunction;
import org.hibernate.dialect.lock.PessimisticLockStyle;
import org.hibernate.dialect.lock.internal.LockingSupportSimple;
import org.hibernate.dialect.lock.spi.ConnectionLockTimeoutStrategy;
import org.hibernate.dialect.lock.spi.LockTimeoutType;
import org.hibernate.dialect.lock.spi.LockingSupport;
import org.hibernate.dialect.lock.spi.OuterJoinLockingType;
import org.hibernate.dialect.sequence.SequenceSupport;
import org.hibernate.dialect.sequence.SpannerPostgreSQLSequenceSupport;
import org.hibernate.dialect.sql.ast.SpannerPostgreSQLSqlAstTranslator;
import org.hibernate.dialect.temptable.PersistentTemporaryTableStrategy;
import org.hibernate.dialect.temptable.TemporaryTableStrategy;
import org.hibernate.dialect.type.SpannerIntegerAsBigIntJdbcType;
import org.hibernate.dialect.type.SpannerSmallIntAsBigIntJdbcType;
import org.hibernate.dialect.type.SpannerTinyIntAsBigIntJdbcType;
import org.hibernate.dialect.type.SpannerZonedOffsetJavaType;
import org.hibernate.dialect.unique.AlterTableUniqueIndexDelegate;
import org.hibernate.dialect.unique.UniqueDelegate;
import org.hibernate.engine.config.spi.ConfigurationService;
import org.hibernate.engine.config.spi.StandardConverters;
import org.hibernate.engine.jdbc.dialect.spi.DialectResolutionInfo;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.exception.ConstraintViolationException;
import org.hibernate.exception.spi.SQLExceptionConversionDelegate;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.Index;
import org.hibernate.mapping.PrimaryKey;
import org.hibernate.mapping.Selectable;
import org.hibernate.mapping.Table;
import org.hibernate.mapping.UniqueKey;
import org.hibernate.mapping.Value;
import org.hibernate.mapping.ValueVisitor;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.metamodel.spi.RuntimeModelCreationContext;
import org.hibernate.procedure.internal.StandardCallableStatementSupport;
import org.hibernate.procedure.spi.CallableStatementSupport;
import org.hibernate.query.sqm.function.SqmFunctionRegistry;
import org.hibernate.query.sqm.mutation.internal.temptable.PersistentTableInsertStrategy;
import org.hibernate.query.sqm.mutation.internal.temptable.PersistentTableMutationStrategy;
import org.hibernate.query.sqm.mutation.spi.SqmMultiTableInsertStrategy;
import org.hibernate.query.sqm.mutation.spi.SqmMultiTableMutationStrategy;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.sql.ast.SqlAstTranslator;
import org.hibernate.sql.ast.SqlAstTranslatorFactory;
import org.hibernate.sql.ast.spi.LockingClauseStrategy;
import org.hibernate.sql.ast.spi.StandardSqlAstTranslatorFactory;
import org.hibernate.sql.ast.tree.Statement;
import org.hibernate.sql.ast.tree.select.QuerySpec;
import org.hibernate.sql.exec.spi.JdbcOperation;
import org.hibernate.tool.schema.internal.StandardTableExporter;
import org.hibernate.tool.schema.spi.Exporter;
import org.hibernate.type.MappingContext;
import org.hibernate.type.Type;
import org.hibernate.type.descriptor.jdbc.JdbcType;
import org.hibernate.type.descriptor.jdbc.spi.JdbcTypeRegistry;
import org.hibernate.type.descriptor.sql.internal.CapacityDependentDdlType;
import org.hibernate.type.descriptor.sql.internal.DdlTypeImpl;
import org.hibernate.type.descriptor.sql.spi.DdlTypeRegistry;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static java.lang.String.join;
import static org.hibernate.sql.ast.internal.NonLockingClauseStrategy.NON_CLAUSE_STRATEGY;
import static org.hibernate.type.SqlTypes.BIGINT;
import static org.hibernate.type.SqlTypes.BLOB;
import static org.hibernate.type.SqlTypes.CHAR;
import static org.hibernate.type.SqlTypes.CLOB;
import static org.hibernate.type.SqlTypes.DOUBLE;
import static org.hibernate.type.SqlTypes.FLOAT;
import static org.hibernate.type.SqlTypes.INTEGER;
import static org.hibernate.type.SqlTypes.NCLOB;
import static org.hibernate.type.SqlTypes.SMALLINT;
import static org.hibernate.type.SqlTypes.TIMESTAMP;
import static org.hibernate.type.SqlTypes.TIMESTAMP_UTC;
import static org.hibernate.type.SqlTypes.TIMESTAMP_WITH_TIMEZONE;
import static org.hibernate.type.SqlTypes.TINYINT;
import static org.hibernate.type.SqlTypes.VARCHAR;

public class SpannerPostgreSQLDialect extends PostgreSQLDialect {

	private final LockingSupport spannerLockingSupport =
			new LockingSupportSimple(
					PessimisticLockStyle.CLAUSE,
					RowLockStrategy.NONE,
					LockTimeoutType.NONE,
					OuterJoinLockingType.FULL,
					ConnectionLockTimeoutStrategy.NONE );

	private static final String USE_TIMESTAMPZ_TYPE_FOR_TIME_TYPE = "hibernate.dialect.spannerpg.use_timestampz_type_for_time_type";

	private boolean useTimestampzForTimeType;

	private final UniqueDelegate SPANNER_UNIQUE_DELEGATE = new AlterTableUniqueIndexDelegate( this );
	private final StandardTableExporter spannerTableExporter = new StandardTableExporter( this ) {
		@Override
		public String[] getSqlCreateStrings(Table table, Metadata metadata, SqlStringGenerationContext context) {
			// Spanner mandates that primary key should be present in all the tables.
			// In order to fix the problem, we randomly generate the ID column with BIT_REVERSED_POSITIVE sequence
			if (!table.hasPrimaryKey()) {
				Column column = getAutoGeneratedPrimaryKeyColumn(table, metadata);
				table.addColumn( column );

				PrimaryKey primaryKey = new PrimaryKey(table);
				primaryKey.addColumn( column );

				table.setPrimaryKey( primaryKey );
			}

			return super.getSqlCreateStrings( table, metadata, context );
		}

		@Override
		public String[] getSqlDropStrings(Table table, Metadata metadata, SqlStringGenerationContext context) {
			// Spanner requires the index to be dropped before dropping the table
			List<String> sqlDropIndexStrings = new ArrayList<>();
			for ( Index index : table.getIndexes().values() ) {
				sqlDropIndexStrings.add(sqlDropIndexString(index.getName()));
			}
			for ( UniqueKey uniqueKey : table.getUniqueKeys().values() ) {
				sqlDropIndexStrings.add(sqlDropIndexString(uniqueKey.getName()));
			}
			for ( Column column : table.getColumns() ) {
				if ( column.isUnique() ) {
					sqlDropIndexStrings.add(sqlDropIndexString(column.getUniqueKeyName()));
				}
			}
			String[] sqlDropStrings = super.getSqlDropStrings( table, metadata, context );
			return Stream.concat( sqlDropIndexStrings.stream(), Stream.of( sqlDropStrings ) )
					.toArray(String[]::new);
		}

		private String sqlDropIndexString(String indexName) {
			return "drop index if exists " + indexName;
		}
	};

	public SpannerPostgreSQLDialect() {
		super();
	}

	public SpannerPostgreSQLDialect(DialectResolutionInfo info) {
		super( info );
	}

	public SpannerPostgreSQLDialect(DatabaseVersion version) {
		super( version );
	}

	@Override
	public void initializeFunctionRegistry(FunctionContributions functionContributions) {
		super.initializeFunctionRegistry( functionContributions );

		SqmFunctionRegistry functionRegistry = functionContributions.getFunctionRegistry();
		final CommonFunctionFactory commonFunctionFactory = new CommonFunctionFactory(functionContributions);

		functionRegistry.register(
				"round", new PostgreSQLTruncRoundFunction( "round", false ));
		// Register Spanner specific function
		commonFunctionFactory.length_characterLength_spanner("");
		commonFunctionFactory.arrayLength_spanner();
		commonFunctionFactory.position_spanner();
		commonFunctionFactory.locate_Spanner();

		// Replace functions
		commonFunctionFactory.jsonArrayAgg_postgresql( false );
		commonFunctionFactory.jsonObjectAgg_postgresql( false );
		commonFunctionFactory.localtimeLocaltimestamp_spanner();

		// Remove unsupported PG functions
		functionRegistry.unregister( "array_append" );
		functionRegistry.unregister( "array_fill" );
		functionRegistry.unregister( "array_fill_list" );
		functionRegistry.unregister( "array_trim" );
		functionRegistry.unregister( "array_set" );
		functionRegistry.unregister( "array_replace" );
		functionRegistry.unregister( "array_remove" );
		functionRegistry.unregister( "array_remove_index" );
		functionRegistry.unregister( "array_prepend" );
		functionRegistry.unregister( "array_position" );
		functionRegistry.unregister( "array_positions" );

		functionRegistry.unregister( "json_query" );
		functionRegistry.unregister( "json_table" );
		functionRegistry.unregister( "json_value" );
		functionRegistry.unregister( "json_exists" );
		functionRegistry.unregister( "json_object" );
		functionRegistry.unregister( "json_objectagg" );
		functionRegistry.unregister( "json_array_append" );
		functionRegistry.unregister( "json_mergepatch" );

		functionRegistry.unregister( "xmlagg" );
		functionRegistry.unregister( "xmlelement" );
		functionRegistry.unregister( "xmlcomment" );
		functionRegistry.unregister( "xmlforest" );
		functionRegistry.unregister( "xmlconcat" );
		functionRegistry.unregister( "xmlpi" );
		functionRegistry.unregister( "xmlquery_postgresql" );
		functionRegistry.unregister( "xmlexists" );
		functionRegistry.unregister( "xmlquery" );
		functionRegistry.unregister( "xmltable" );

		functionRegistry.unregister( "generate_series" );
	}

	@Override
	protected DatabaseVersion getMinimumSupportedVersion() {
		return SimpleDatabaseVersion.ZERO_VERSION;
	}

	@Override
	public void contributeTypes(TypeContributions typeContributions, ServiceRegistry serviceRegistry) {
		super.contributeTypes( typeContributions, serviceRegistry );

		typeContributions.getTypeConfiguration().getJavaTypeRegistry().addDescriptor( SpannerZonedOffsetJavaType.INSTANCE );

		typeContributions.getTypeConfiguration().getJdbcTypeRegistry().addDescriptor( SpannerIntegerAsBigIntJdbcType.INSTANCE );
		typeContributions.getTypeConfiguration().getJdbcTypeRegistry().addDescriptor( SpannerSmallIntAsBigIntJdbcType.INSTANCE );
		typeContributions.getTypeConfiguration().getJdbcTypeRegistry().addDescriptor( SpannerTinyIntAsBigIntJdbcType.INSTANCE );
	}


	@Override
	protected void registerColumnTypes(
			TypeContributions typeContributions, ServiceRegistry serviceRegistry) {

		final ConfigurationService configurationService = serviceRegistry.getService( ConfigurationService.class );
		if (configurationService != null) {
			this.useTimestampzForTimeType = configurationService.getSetting( USE_TIMESTAMPZ_TYPE_FOR_TIME_TYPE,
					StandardConverters.BOOLEAN,
					this.useTimestampzForTimeType );
		}

		super.registerColumnTypes( typeContributions, serviceRegistry );

		final DdlTypeRegistry ddlTypeRegistry =
				typeContributions.getTypeConfiguration().getDdlTypeRegistry();

		ddlTypeRegistry.addDescriptor( new DdlTypeImpl( Types.NUMERIC, "numeric", this ) );
		ddlTypeRegistry.addDescriptor( new DdlTypeImpl( Types.DECIMAL, "decimal", this ) );
		ddlTypeRegistry.addDescriptor( new DdlTypeImpl( Types.TINYINT, "bigint", this ) );

		ddlTypeRegistry.addDescriptor(
				CapacityDependentDdlType.builder( FLOAT, columnType( FLOAT ), castType( FLOAT ), this )
						.withTypeCapacity( 24, "real" )
						.withTypeCapacity( 53, "double precision" )
						.build()
		);
	}

	@Override
	public SqlAstTranslatorFactory getSqlAstTranslatorFactory() {
		return new StandardSqlAstTranslatorFactory() {
			@Override
			protected <T extends JdbcOperation> SqlAstTranslator<T> buildTranslator(
					SessionFactoryImplementor sessionFactory, Statement statement) {
				return new SpannerPostgreSQLSqlAstTranslator<>( sessionFactory, statement );
			}
		};
	}

	@Override
	public String getArrayTypeName(String javaElementTypeName, String elementTypeName, Integer maxLength) {
		if(elementTypeName != null && elementTypeName.equals( "varchar" )) {
			elementTypeName = "text";
		}
		return super.getArrayTypeName( javaElementTypeName, elementTypeName, maxLength );
	}

	@Override
	public AggregateSupport getAggregateSupport() {
		return SpannerPostgreSQLAggregateSupport.INSTANCE;
	}

	@Override
	public boolean supportsUserDefinedTypes() {
		return false;
	}

	@Override
	public boolean supportsFilterClause() {
		return false;
	}

	@Override
	public boolean supportsRecursiveCycleUsingClause() {
		return false;
	}

	@Override
	public boolean supportsRecursiveSearchClause() {
		return false;
	}

	@Override
	public boolean supportsUniqueConstraintInColumnDefinition() {
		return false;
	}

	@Override
	public boolean supportsRowValueConstructorGtLtSyntax() {
		return false;
	}

	// ALL subqueries with operators other than <>/!= are not supported
	@Override
	public boolean supportsRowValueConstructorSyntaxInQuantifiedPredicates() {
		return false;
	}

	@Override
	public boolean supportsRowValueConstructorSyntaxInInSubQuery() {
		return false;
	}

	@Override
	public boolean supportsCaseInsensitiveLike() {
		return false;
	}

	@Override
	public Exporter<Table> getTableExporter() {
		return spannerTableExporter;
	}

	@Override
	public String currentTimestamp() {
		return currentTimestampWithTimeZone();
	}

	@Override
	public String currentTime() {
		return currentTimestampWithTimeZone();
	}

	@Override
	public boolean supportsLateral() {
		return false;
	}

	@Override
	public boolean supportsFromClauseInUpdate() {
		return false;
	}

	@Override
	public int getMaxVarcharLength() {
		//max is equivalent to 2_621_440
		return 2_621_440;
	}

	@Override
	public int getMaxVarbinaryLength() {
		//max is equivalent 10 MiB
		return 10_485_760;
	}

	@Override
	public boolean canCreateSchema() {
		return false;
	}

	@Override
	public String[] getCreateSchemaCommand(String schemaName) {
		throw new UnsupportedOperationException(
				"No create schema syntax supported by " + getClass().getName() );
	}

	@Override
	public String[] getDropSchemaCommand(String schemaName) {
		throw new UnsupportedOperationException(
				"No drop schema syntax supported by " + getClass().getName() );
	}

	@Override
	public String getCurrentSchemaCommand() {
		return "";
	}

	@Override
	public SqmMultiTableMutationStrategy getFallbackSqmMutationStrategy(
			EntityMappingType rootEntityDescriptor,
			RuntimeModelCreationContext runtimeModelCreationContext) {
		return new PersistentTableMutationStrategy( rootEntityDescriptor, runtimeModelCreationContext );
	}

	@Override
	public SqmMultiTableInsertStrategy getFallbackSqmInsertStrategy(
			EntityMappingType rootEntityDescriptor,
			RuntimeModelCreationContext runtimeModelCreationContext) {
		return new PersistentTableInsertStrategy( rootEntityDescriptor, runtimeModelCreationContext );
	}

	@Override
	public TemporaryTableStrategy getLocalTemporaryTableStrategy() {
		return null;
	}

	@Override
	public @Nullable TemporaryTableStrategy getGlobalTemporaryTableStrategy() {
		return null;
	}

	@Override
	public TemporaryTableStrategy getPersistentTemporaryTableStrategy() {
		return new PersistentTemporaryTableStrategy( this );
	}

	@Override
	protected String columnType(int sqlTypeCode) {
		if (useTimestampzForTimeType && (sqlTypeCode == Types.TIME || sqlTypeCode == Types.TIMESTAMP)) {
			return columnType( TIMESTAMP_WITH_TIMEZONE );
		}

		return switch (sqlTypeCode) {
			case TIMESTAMP_UTC, TIMESTAMP_WITH_TIMEZONE -> "timestamp with time zone";
			case BLOB -> "bytea";
			case DOUBLE -> "double precision";
			case CLOB, NCLOB -> "character varying";
			case CHAR -> columnType( VARCHAR );
			case SMALLINT, INTEGER, TINYINT ->  columnType( BIGINT );
			default -> super.columnType(sqlTypeCode);
		};
	}

	@Override
	public String getAddForeignKeyConstraintString(
			String constraintName,
			String[] foreignKey,
			String referencedTable,
			String[] primaryKey,
			boolean referencesPrimaryKey) {
		final StringBuilder res = new StringBuilder( 30 );
		// Reference Primary key always
		res.append( " add constraint " )
				.append( quote( constraintName ) )
				.append( " foreign key (" )
				.append( join( ", ", foreignKey ) )
				.append( ") references " )
				.append( referencedTable )
				.append( " (" )
				.append( join( ", ", primaryKey ) )
				.append( ')' );

		return res.toString();
	}

	@Override
	public JdbcType resolveSqlTypeDescriptor(
			String columnTypeName,
			int jdbcTypeCode,
			int precision,
			int scale,
			JdbcTypeRegistry jdbcTypeRegistry) {
		// Spanner only supports type with timezone
		if ( jdbcTypeCode == TIMESTAMP ) {
			if ( columnTypeName.equals( "timestamp with time zone" ) ) {
				jdbcTypeCode = TIMESTAMP_WITH_TIMEZONE;
			}
		}

		return super.resolveSqlTypeDescriptor(
				columnTypeName, jdbcTypeCode, precision, scale, jdbcTypeRegistry );
	}

	@Override
	public boolean canBatchTruncate() {
		return false;
	}

	@Override
	public String rowId(String rowId) {
		return null;
	}

	@Override
	public boolean supportsRowConstructor() {
		return false;
	}

	@Override
	public String getTruncateTableStatement(String tableName) {
		return "delete from " + tableName;
	}

	@Override
	public String getBeforeDropStatement() {
		return null;
	}

	@Override
	public String getCascadeConstraintsString() {
		return "";
	}

	@Override
	public SequenceSupport getSequenceSupport() {
		return SpannerPostgreSQLSequenceSupport.INSTANCE;
	}

	@Override
	public boolean supportsIfExistsBeforeConstraintName() {
		return false;
	}

	@Override
	public boolean supportsIfExistsAfterAlterTable() {
		return false;
	}

	@Override
	public String getForUpdateString() {
		return " for update";
	}

	@Override
	public String getForUpdateString(String aliases) {
		return getForUpdateString();
	}

	@Override
	public LockingSupport getLockingSupport() {
		return spannerLockingSupport;
	}

	@Override
	public String getWriteLockString(int timeout) {
		validateSpannerLockTimeout( timeout );
		return getForUpdateString();
	}

	@Override
	public String getWriteLockString(String aliases, int timeout) {
		return getWriteLockString( timeout );
	}

	@Override
	public String getWriteLockString(Timeout timeout) {
		return getWriteLockString( timeout.milliseconds() );
	}

	@Override
	public String getWriteLockString(String aliases, Timeout timeout) {
		return getWriteLockString( timeout.milliseconds() );
	}

	@Override
	public String getReadLockString(int timeout) {
		validateSpannerLockTimeout( timeout );
		return getForUpdateString();
	}

	@Override
	public String getReadLockString(Timeout timeout) {
		return getWriteLockString( timeout.milliseconds() );
	}

	@Override
	public String getReadLockString(String aliases, Timeout timeout) {
		return getWriteLockString( timeout.milliseconds() );
	}

	@Override
	public String getReadLockString(String aliases, int timeout) {
		return getWriteLockString( timeout );
	}

	@Override
	public String getForUpdateNowaitString() {
		throw new UnsupportedOperationException(
				"Spanner doesn't support for-update with no-wait timeout" );
	}

	@Override
	public String getForUpdateNowaitString(String aliases) {
		return getForUpdateString();
	}

	@Override
	public String getForUpdateSkipLockedString() {
		throw new UnsupportedOperationException(
				"Spanner doesn't support for-update with skip locked timeout" );
	}

	@Override
	public String getForUpdateSkipLockedString(String aliases) {
		return getForUpdateSkipLockedString();
	}

	@Override
	public LockingClauseStrategy getLockingClauseStrategy(
			QuerySpec querySpec, LockOptions lockOptions) {
		if ( lockOptions == null ) {
			return NON_CLAUSE_STRATEGY;
		}
		validateSpannerLockTimeout( lockOptions.getTimeOut() );
		return super.getLockingClauseStrategy( querySpec, lockOptions );
	}

	private static void validateSpannerLockTimeout(int millis) {
		if ( Timeouts.isRealTimeout( millis ) ) {
			throw new UnsupportedOperationException( "Spanner does not support lock timeout." );
		}
		if ( millis == Timeouts.SKIP_LOCKED_MILLI ) {
			throw new UnsupportedOperationException( "Spanner does not support skip locked." );
		}
		if ( millis == Timeouts.NO_WAIT_MILLI ) {
			throw new UnsupportedOperationException( "Spanner does not support no wait." );
		}
	}

	@Override
	public boolean supportsDistinctFromPredicate() {
		return false;
	}

	@Override
	public boolean supportsPartitionBy() {
		return false;
	}

	@Override
	public boolean supportsNonQueryWithCTE() {
		return false;
	}

	@Override
	public boolean supportsRecursiveCTE() {
		return false;
	}

	@Override
	public CallableStatementSupport getCallableStatementSupport() {
		// most databases do not support returning cursors (ref_cursor)...
		return StandardCallableStatementSupport.NO_REF_CURSOR_INSTANCE;
	}

	@Override
	public UniqueDelegate getUniqueDelegate() {
		return SPANNER_UNIQUE_DELEGATE;
	}

	// TODO(sakthivelmani): Handle the error message through REGEX
	@Override
	public SQLExceptionConversionDelegate buildSQLExceptionConversionDelegate() {
		return this::handleConstraintViolatedException;
	}

	private @Nullable JDBCException handleConstraintViolatedException(SQLException sqlException, String message, String sql) {
		if (sqlException.getErrorCode() == 6 || (message != null && message.contains("Cannot specify a null value for column"))) {
			return new ConstraintViolationException(message, sqlException, sql);
		} else if (message != null && message.contains( "does not specify a non-null value for NOT NULL column" )) {
			return new ConstraintViolationException(message, sqlException, ConstraintViolationException.ConstraintKind.NOT_NULL, null);
		} else if (sqlException.getErrorCode() == 11 || (message != null && message.contains("Check constraint"))) {
			return new ConstraintViolationException(message, sqlException, ConstraintViolationException.ConstraintKind.CHECK, null);
		} else if(message != null && message.contains( "Foreign key" ) &&
				(message.contains( " constraint violation on table" ) ||
				message.contains( "constraint violation when deleting or updating referenced key"))) {
			return new ConstraintViolationException( message, sqlException, ConstraintViolationException.ConstraintKind.FOREIGN_KEY, null );
		} else {
			return null;
		}
	}

	private Column getAutoGeneratedPrimaryKeyColumn(Table table, Metadata metadata) {
		Column column = new Column("rowid");
		column.setSqlTypeCode(  Types.BIGINT  );
		column.setNullable( false );
		column.setSqlType( "bigint" );
		column.setOptions( "hidden" );
		column.setIdentity(  true );
		column.setValue( new Value() {
			@Override
			public int getColumnSpan() {
				return 1;
			}

			@Override
			public List<Selectable> getSelectables() {
				return List.of(column);
			}

			@Override
			public List<Column> getColumns() {
				return List.of(column);
			}

			@Override
			public boolean hasColumns() {
				return true;
			}

			@Override
			public Type getType() throws MappingException {
				return metadata.getDatabase().getTypeConfiguration().getBasicTypeForJavaType( Long.class );
			}

			@Override
			public FetchMode getFetchMode() {
				return null;
			}

			@Override
			public Table getTable() {
				return table;
			}

			@Override
			public boolean hasFormula() {
				return false;
			}

			@Override
			public boolean isAlternateUniqueKey() {
				return false;
			}

			@Override
			public boolean isPartitionKey() {
				return false;
			}

			@Override
			public boolean isNullable() {
				return false;
			}

			@Override
			public void createForeignKey() {

			}

			@Override
			public void createUniqueKey(MetadataBuildingContext context) {

			}

			@Override
			public boolean isSimpleValue() {
				return false;
			}

			@Override
			public boolean isValid(MappingContext mappingContext) throws MappingException {
				return false;
			}

			@Override
			public void setTypeUsingReflection(String className, String propertyName) throws MappingException {

			}

			@Override
			public Object accept(ValueVisitor visitor) {
				return null;
			}

			@Override
			public boolean isSame(Value other) {
				return false;
			}

			@Override
			public boolean[] getColumnInsertability() {
				return new boolean[0];
			}

			@Override
			public boolean hasAnyInsertableColumns() {
				return false;
			}

			@Override
			public boolean[] getColumnUpdateability() {
				return new boolean[0];
			}

			@Override
			public boolean hasAnyUpdatableColumns() {
				return false;
			}

			@Override
			public ServiceRegistry getServiceRegistry() {
				return metadata.getDatabase().getServiceRegistry();
			}

			@Override
			public Value copy() {
				return null;
			}

			@Override
			public boolean isColumnInsertable(int index) {
				return false;
			}

			@Override
			public boolean isColumnUpdateable(int index) {
				return false;
			}
		} );
		return column;
	}
}
