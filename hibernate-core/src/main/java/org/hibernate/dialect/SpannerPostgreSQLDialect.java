/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.dialect;

import jakarta.persistence.Timeout;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.hibernate.FetchMode;
import org.hibernate.LockOptions;
import org.hibernate.MappingException;
import org.hibernate.Timeouts;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.model.FunctionContributions;
import org.hibernate.boot.model.TypeContributions;
import org.hibernate.boot.model.relational.SqlStringGenerationContext;
import org.hibernate.boot.spi.MetadataBuildingContext;
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
import org.hibernate.dialect.type.SpannerIntegerAsBigIntType;
import org.hibernate.dialect.unique.CreateTableUniqueDelegate;
import org.hibernate.dialect.unique.UniqueDelegate;
import org.hibernate.engine.jdbc.dialect.spi.DialectResolutionInfo;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.exception.ConstraintViolationException;
import org.hibernate.exception.spi.SQLExceptionConversionDelegate;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.PrimaryKey;
import org.hibernate.mapping.Selectable;
import org.hibernate.mapping.Table;
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
import org.hibernate.type.descriptor.sql.internal.DdlTypeImpl;
import org.hibernate.type.descriptor.sql.spi.DdlTypeRegistry;

import java.sql.Types;
import java.util.List;

import static java.lang.String.join;
import static org.hibernate.sql.ast.internal.NonLockingClauseStrategy.NON_CLAUSE_STRATEGY;
import static org.hibernate.type.SqlTypes.BIGINT;
import static org.hibernate.type.SqlTypes.BLOB;
import static org.hibernate.type.SqlTypes.CHAR;
import static org.hibernate.type.SqlTypes.CLOB;
import static org.hibernate.type.SqlTypes.INTEGER;
import static org.hibernate.type.SqlTypes.NCLOB;
import static org.hibernate.type.SqlTypes.SMALLINT;
import static org.hibernate.type.SqlTypes.TIME;
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
	private final UniqueDelegate SPANNER_UNIQUE_DELEGATE = new SpannerUniqueDelegate( this );
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

		functionRegistry.register(
				"round", new PostgreSQLTruncRoundFunction( "round", false ));

		// Remove unsupported PG functions

		functionRegistry.unregister( "array_append" );
		functionRegistry.register(
				"round", new PostgreSQLTruncRoundFunction( "round", true )
		);

		functionRegistry.unregister( "xmlagg" );
		functionRegistry.unregister( "xmlelement" );
		functionRegistry.unregister( "xmlcomment" );
		functionRegistry.unregister( "xmlforest" );
		functionRegistry.unregister( "xmlconcat" );
		functionRegistry.unregister( "xmlpi" );
		functionRegistry.unregister( "xmlquery_postgresql" );
		functionRegistry.unregister( "xmlexists" );
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

		typeContributions.getTypeConfiguration().getJdbcTypeRegistry().addDescriptor( SpannerIntegerAsBigIntType.INSTANCE );
	}

	@Override
	protected void registerColumnTypes(
			TypeContributions typeContributions, ServiceRegistry serviceRegistry) {
		super.registerColumnTypes( typeContributions, serviceRegistry );

		final DdlTypeRegistry ddlTypeRegistry =
				typeContributions.getTypeConfiguration().getDdlTypeRegistry();

		ddlTypeRegistry.addDescriptor( new DdlTypeImpl( Types.FLOAT, "real", this ) );
		ddlTypeRegistry.addDescriptor( new DdlTypeImpl( Types.NUMERIC, "numeric", this ) );
		ddlTypeRegistry.addDescriptor( new DdlTypeImpl( Types.TINYINT, "bigint", this ) );
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
	public boolean supportsUniqueConstraintInColumnDefinition() {
		return false;
	}

	@Override
	public boolean supportsRowValueConstructorSyntax() {
		return false;
	}

	@Override
	public boolean supportsRowValueConstructorSyntaxInInList() {
		return false;
	}

	@Override
	public boolean supportsRowValueConstructorSyntaxInQuantifiedPredicates() {
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
	public boolean supportsWindowFunctions() {
		return false;
	}

	@Override
	public boolean supportsLateral() {
		return false;
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
		return switch (sqlTypeCode) {
			case TIME, TIMESTAMP, TIMESTAMP_UTC, TIMESTAMP_WITH_TIMEZONE -> "timestamp with time zone";
			case BLOB -> "bytea";
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
	public CallableStatementSupport getCallableStatementSupport() {
		// most databases do not support returning cursors (ref_cursor)...
		return StandardCallableStatementSupport.NO_REF_CURSOR_INSTANCE;
	}

	@Override
	public UniqueDelegate getUniqueDelegate() {
		return SPANNER_UNIQUE_DELEGATE;
	}

	static class SpannerUniqueDelegate extends CreateTableUniqueDelegate {

		public SpannerUniqueDelegate(Dialect dialect) {
			super( dialect );
		}

		@Override
		public String getColumnDefinitionUniquenessFragment(Column column, SqlStringGenerationContext context) {
			return "";
		}

		@Override
		public String getTableCreationUniqueConstraintsFragment(Table table, SqlStringGenerationContext context) {
			return "";
		}
	}

	@Override
	public SQLExceptionConversionDelegate buildSQLExceptionConversionDelegate() {
		return (sqlException, message, sql) -> {
			return switch ( sqlException.getErrorCode() ) {
				case 6 ->
						// ALREADY EXISTS
						new ConstraintViolationException( message, sqlException, sql );
				default -> null;
			};
		};
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
