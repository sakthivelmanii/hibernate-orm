/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.dialect;

import jakarta.persistence.Timeout;
import org.hibernate.LockOptions;
import org.hibernate.Timeouts;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.model.TypeContributions;
import org.hibernate.boot.model.relational.SqlStringGenerationContext;
import org.hibernate.dialect.lock.PessimisticLockStyle;
import org.hibernate.dialect.lock.internal.LockingSupportSimple;
import org.hibernate.dialect.lock.spi.ConnectionLockTimeoutStrategy;
import org.hibernate.dialect.lock.spi.LockTimeoutType;
import org.hibernate.dialect.lock.spi.LockingSupport;
import org.hibernate.dialect.lock.spi.OuterJoinLockingType;
import org.hibernate.dialect.sequence.SequenceSupport;
import org.hibernate.dialect.sequence.SpannerPostgreSQLSequenceSupport;
import org.hibernate.dialect.sql.ast.SpannerPostgreSQLSqlAstTranslator;
import org.hibernate.dialect.unique.UniqueDelegate;
import org.hibernate.engine.jdbc.dialect.spi.DialectResolutionInfo;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.Table;
import org.hibernate.mapping.UniqueKey;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.sql.ast.SqlAstTranslator;
import org.hibernate.sql.ast.SqlAstTranslatorFactory;
import org.hibernate.sql.ast.spi.LockingClauseStrategy;
import org.hibernate.sql.ast.spi.StandardSqlAstTranslatorFactory;
import org.hibernate.sql.ast.tree.Statement;
import org.hibernate.sql.ast.tree.select.QuerySpec;
import org.hibernate.sql.exec.spi.JdbcOperation;
import org.hibernate.type.SqlTypes;
import org.hibernate.type.descriptor.jdbc.JdbcType;
import org.hibernate.type.descriptor.jdbc.spi.JdbcTypeRegistry;
import org.hibernate.type.descriptor.sql.internal.DdlTypeImpl;
import org.hibernate.type.descriptor.sql.spi.DdlTypeRegistry;

import java.sql.Types;

import static java.lang.String.join;
import static org.hibernate.sql.ast.internal.NonLockingClauseStrategy.NON_CLAUSE_STRATEGY;

public class SpannerPostgreSQLDialect extends PostgreSQLDialect {

	private static final UniqueDelegate NOOP_UNIQUE_DELEGATE = new DoNothingUniqueDelegate();

	LockingSupport spannerLockingSupport =
			new LockingSupportSimple(
					PessimisticLockStyle.CLAUSE,
					RowLockStrategy.NONE,
					LockTimeoutType.NONE,
					OuterJoinLockingType.FULL,
					ConnectionLockTimeoutStrategy.NONE );

	public SpannerPostgreSQLDialect() {
		super();
	}

	public SpannerPostgreSQLDialect(DialectResolutionInfo info) {
		super( info );
	}

	public SpannerPostgreSQLDialect(DatabaseVersion version) {
		super( version );
	}

	public SpannerPostgreSQLDialect(DatabaseVersion version, PostgreSQLDriverKind driverKind) {
		super( version, driverKind );
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
		if ( jdbcTypeCode == SqlTypes.TIMESTAMP ) {
			if ( columnTypeName.equals( "timestamp with time zone" ) ) {
				jdbcTypeCode = SqlTypes.TIMESTAMP_WITH_TIMEZONE;
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
	public UniqueDelegate getUniqueDelegate() {
		return NOOP_UNIQUE_DELEGATE;
	}

	/**
	 * A no-op delegate for generating Unique-Constraints. Cloud Spanner offers unique-restrictions
	 * via interleaved indexes with the "UNIQUE" option. This is not currently supported.
	 *
	 * @author Chengyuan Zhao
	 */
	static class DoNothingUniqueDelegate implements UniqueDelegate {

		@Override
		public String getColumnDefinitionUniquenessFragment(Column column, SqlStringGenerationContext context) {
			return "";
		}

		@Override
		public String getTableCreationUniqueConstraintsFragment(Table table, SqlStringGenerationContext context) {
			return "";
		}

		@Override
		public String getAlterTableToAddUniqueKeyCommand(UniqueKey uniqueKey, Metadata metadata, SqlStringGenerationContext context) {
			return "";
		}

		@Override
		public String getAlterTableToDropUniqueKeyCommand(UniqueKey uniqueKey, Metadata metadata, SqlStringGenerationContext context) {
			return "";
		}
	}
}
