/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.dialect.schema.internal;

import org.hibernate.boot.Metadata;
import org.hibernate.boot.model.relational.QualifiedNameImpl;
import org.hibernate.boot.model.relational.SqlStringGenerationContext;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.SpannerPostgreSQLDialect;
import org.hibernate.dialect.schema.spi.IndexNameQualification;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.Index;
import org.hibernate.mapping.Selectable;
import org.hibernate.tool.schema.spi.Exporter;
import org.hibernate.tool.schema.spi.StandardIndexExporter;
import org.hibernate.type.SqlTypes;

import static org.hibernate.internal.util.StringHelper.isNotBlank;
import static org.hibernate.internal.util.StringHelper.qualify;

/// Specialized index exporter for Cloud Spanner PostgreSQL ([SpannerPostgreSQLDialect])
/// supporting vector indexes for approximate nearest neighbor (ANN) similarity search.
///
/// Spanner PostgreSQL uses `create index <name> on <table> using scann (<col>) with (...)`
/// and `drop index if exists <name>`.
///
/// Standard non-vector indexes are delegated to [StandardIndexExporter].
public class SpannerPostgreSQLIndexExporter implements Exporter<Index> {

	private final Dialect dialect;
	private final StandardIndexExporter standardExporter;

	public SpannerPostgreSQLIndexExporter(SpannerPostgreSQLDialect dialect) {
		this( (Dialect) dialect );
	}

	public SpannerPostgreSQLIndexExporter(Dialect dialect) {
		this.dialect = dialect;
		this.standardExporter = new StandardIndexExporter( dialect );
	}

	public boolean isVectorIndex(Index index, Metadata metadata) {
		if ( index == null ) {
			return false;
		}
		if ( "vector".equalsIgnoreCase( index.getType() ) || "scann".equalsIgnoreCase( index.getType() ) ) {
			return true;
		}
		if ( "scann".equalsIgnoreCase( index.getUsing() ) || "vector".equalsIgnoreCase( index.getUsing() ) ) {
			return true;
		}
		for ( Selectable selectable : index.getSelectables() ) {
			if ( selectable instanceof Column col ) {
				Integer code = col.getSqlTypeCode();
				if ( code == null && metadata != null ) {
					try {
						code = col.getSqlTypeCode( metadata );
					}
					catch (Exception ignored) {
					}
				}
				if ( code != null && isVectorSqlType( code ) ) {
					return true;
				}
			}
		}
		return false;
	}

	public static boolean isVectorSqlType(int code) {
		return code == SqlTypes.VECTOR
				|| code == SqlTypes.VECTOR_FLOAT32
				|| code == SqlTypes.VECTOR_FLOAT64
				|| code == SqlTypes.VECTOR_INT8
				|| code == SqlTypes.VECTOR_BINARY
				|| code == SqlTypes.VECTOR_FLOAT16;
	}

	@Override
	public String[] getSqlCreateStrings(Index index, Metadata metadata, SqlStringGenerationContext context) {
		if ( isVectorIndex( index, metadata ) ) {
			return new String[] { buildVectorIndexDdl( index, metadata, context ) };
		}
		return standardExporter.getSqlCreateStrings( index, metadata, context );
	}

	@Override
	public String[] getSqlDropStrings(Index index, Metadata metadata, SqlStringGenerationContext context) {
		if ( isVectorIndex( index, metadata ) ) {
			return new String[] { buildVectorIndexDropDdl( index, metadata, context ) };
		}
		return standardExporter.getSqlDropStrings( index, metadata, context );
	}

	private String buildVectorIndexDdl(Index index, Metadata metadata, SqlStringGenerationContext context) {
		final StringBuilder ddl = new StringBuilder( "create index " )
				.append( getIndexName( index, context, metadata ) )
				.append( " on " )
				.append( context.format( index.getTable().getQualifiedTableName() ) )
				.append( " using scann (" );

		boolean first = true;
		for ( Selectable selectable : index.getSelectables() ) {
			if ( !first ) {
				ddl.append( ", " );
			}
			first = false;
			ddl.append( selectable.getText( dialect ) );
		}
		ddl.append( ")" );

		final String options = index.getOptions();
		if ( isNotBlank( options ) ) {
			final String trimmed = options.trim();
			if ( trimmed.toLowerCase().startsWith( "with" ) ) {
				ddl.append( " " ).append( trimmed );
			}
			else if ( trimmed.toLowerCase().startsWith( "options" ) ) {
				ddl.append( " with " ).append( trimmed.substring( "options".length() ).trim() );
			}
			else if ( trimmed.startsWith( "(" ) ) {
				ddl.append( " with " ).append( trimmed );
			}
			else {
				ddl.append( " with (" ).append( trimmed ).append( ")" );
			}
		}
		else {
			ddl.append( " with (distance_type = 'COSINE')" );
		}
		return ddl.toString();
	}

	private String buildVectorIndexDropDdl(Index index, Metadata metadata, SqlStringGenerationContext context) {
		return "drop index if exists " + getIndexName( index, context, metadata );
	}

	private String getIndexName(Index index, SqlStringGenerationContext context, Metadata metadata) {
		if ( dialect.getIndexDdlSupport().nameQualification() == IndexNameQualification.QUALIFIED ) {
			final var qualifiedTableName = index.getTable().getQualifiedTableName();
			if ( metadata != null && metadata.getDatabase() != null && metadata.getDatabase().getJdbcEnvironment() != null ) {
				return context.format(
						new QualifiedNameImpl(
								qualifiedTableName.getCatalogName(),
								qualifiedTableName.getSchemaName(),
								metadata.getDatabase().getJdbcEnvironment().getIdentifierHelper()
										.toIdentifier( index.getQuotedName( dialect ) )
						)
				);
			}
			else {
				return qualify( context.format( qualifiedTableName ), index.getName() );
			}
		}
		else {
			return index.getName();
		}
	}
}
