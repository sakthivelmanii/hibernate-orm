/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.dialect.schema.internal;

import java.sql.Types;

import org.hibernate.boot.model.naming.Identifier;
import org.hibernate.boot.model.relational.QualifiedName;
import org.hibernate.boot.model.relational.QualifiedSequenceName;
import org.hibernate.boot.model.relational.QualifiedTableName;
import org.hibernate.boot.model.relational.SqlStringGenerationContext;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.SpannerDialect;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.Index;
import org.hibernate.mapping.Table;
import org.hibernate.testing.orm.junit.BaseUnitTest;
import org.hibernate.type.SqlTypes;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@BaseUnitTest
public class SpannerIndexExporterTest {

	@Test
	public void testGoogleSqlVectorIndexCreateNullableColumn() {
		final Dialect dialect = new SpannerDialect();
		final Table table = new Table( "test", "documents" );
		final Column col = new Column( "embedding" );
		col.setNullable( true );
		col.setSqlTypeCode( SqlTypes.VECTOR_FLOAT32 );

		final Index index = table.getOrCreateIndex( "idx_doc_embedding" );
		index.addColumn( col );

		final String[] ddl = dialect.getIndexExporter().getSqlCreateStrings( index, null, new TestContext( dialect ) );
		assertThat( ddl ).containsExactly(
				"create vector index idx_doc_embedding on documents (embedding) where embedding is not null options (distance_type = 'COSINE')"
		);
	}

	@Test
	public void testGoogleSqlVectorIndexCreateNonNullableColumn() {
		final Dialect dialect = new SpannerDialect();
		final Table table = new Table( "test", "documents" );
		final Column col = new Column( "embedding" );
		col.setNullable( false );
		col.setSqlTypeCode( SqlTypes.VECTOR_FLOAT32 );

		final Index index = table.getOrCreateIndex( "idx_doc_embedding" );
		index.addColumn( col );

		final String[] ddl = dialect.getIndexExporter().getSqlCreateStrings( index, null, new TestContext( dialect ) );
		assertThat( ddl ).containsExactly(
				"create vector index idx_doc_embedding on documents (embedding) options (distance_type = 'COSINE')"
		);
	}

	@Test
	public void testGoogleSqlVectorIndexCreateCustomOptions() {
		final Dialect dialect = new SpannerDialect();
		final Table table = new Table( "test", "documents" );
		final Column col = new Column( "embedding" );
		col.setNullable( true );
		col.setSqlTypeCode( SqlTypes.VECTOR_FLOAT32 );

		final Index index = table.getOrCreateIndex( "idx_doc_embedding" );
		index.addColumn( col );
		index.setOptions( "options (distance_type = 'EUCLIDEAN')" );

		final String[] ddl = dialect.getIndexExporter().getSqlCreateStrings( index, null, new TestContext( dialect ) );
		assertThat( ddl ).containsExactly(
				"create vector index idx_doc_embedding on documents (embedding) where embedding is not null options (distance_type = 'EUCLIDEAN')"
		);
	}

	@Test
	public void testGoogleSqlVectorIndexDrop() {
		final Dialect dialect = new SpannerDialect();
		final Table table = new Table( "test", "documents" );
		final Column col = new Column( "embedding" );
		col.setSqlTypeCode( SqlTypes.VECTOR_FLOAT32 );

		final Index index = table.getOrCreateIndex( "idx_doc_embedding" );
		index.addColumn( col );

		final String[] ddl = dialect.getIndexExporter().getSqlDropStrings( index, null, new TestContext( dialect ) );
		assertThat( ddl ).containsExactly(
				"drop vector index if exists idx_doc_embedding"
		);
	}

	@Test
	public void testGoogleSqlStandardIndexCreateAndDrop() {
		final Dialect dialect = new SpannerDialect();
		final Table table = new Table( "test", "orders" );
		final Column col = new Column( "customer" );
		col.setSqlTypeCode( Types.VARCHAR );

		final Index index = table.getOrCreateIndex( "idx_orders_customer" );
		index.addColumn( col );

		final String[] createDdl = dialect.getIndexExporter().getSqlCreateStrings( index, null, new TestContext( dialect ) );
		assertThat( createDdl ).containsExactly(
				"create index idx_orders_customer on orders (customer)"
		);

		final String[] dropDdl = dialect.getIndexExporter().getSqlDropStrings( index, null, new TestContext( dialect ) );
		assertThat( dropDdl ).containsExactly(
				"drop index idx_orders_customer"
		);
	}

	@Test
	public void testGoogleSqlStandardUniqueIndex() {
		final Dialect dialect = new SpannerDialect();
		final Table table = new Table( "test", "orders" );
		final Column col = new Column( "code" );
		col.setSqlTypeCode( Types.VARCHAR );

		final Index index = table.getOrCreateIndex( "idx_orders_code" );
		index.addColumn( col );
		index.setUnique( true );

		final String[] createDdl = dialect.getIndexExporter().getSqlCreateStrings( index, null, new TestContext( dialect ) );
		assertThat( createDdl ).containsExactly(
				"create unique null_filtered index idx_orders_code on orders (code)"
		);
	}

	@Test
	public void testVectorIndexDetectedByExplicitType() {
		final Dialect dialect = new SpannerDialect();
		final Table table = new Table( "test", "documents" );
		final Column col = new Column( "embedding" );
		col.setNullable( true );

		final Index index = table.getOrCreateIndex( "idx_doc_embedding" );
		index.addColumn( col );
		index.setType( "vector" );

		final String[] ddl = dialect.getIndexExporter().getSqlCreateStrings( index, null, new TestContext( dialect ) );
		assertThat( ddl ).containsExactly(
				"create vector index idx_doc_embedding on documents (embedding) where embedding is not null options (distance_type = 'COSINE')"
		);
	}

	@Test
	public void testSpannerDialectTableExporterDropWithVectorIndex() {
		final Dialect dialect = new SpannerDialect();
		final Table table = new Table( "test", "documents" );

		final Column titleCol = new Column( "title" );
		titleCol.setSqlTypeCode( Types.VARCHAR );
		final Index titleIndex = table.getOrCreateIndex( "idx_doc_title" );
		titleIndex.addColumn( titleCol );

		final Column vecCol = new Column( "embedding" );
		vecCol.setSqlTypeCode( SqlTypes.VECTOR_FLOAT32 );
		final Index vecIndex = table.getOrCreateIndex( "idx_doc_embedding" );
		vecIndex.addColumn( vecCol );

		final String[] dropDdl = dialect.getTableExporter().getSqlDropStrings( table, null, new TestContext( dialect ) );
		assertThat( dropDdl ).containsExactly(
				"drop index if exists idx_doc_title",
				"drop vector index if exists idx_doc_embedding",
				"drop table if exists documents"
		);
	}

	@Test
	public void testDialectExposesSpannerIndexExporter() {
		final Dialect gsqlDialect = new SpannerDialect();
		assertThat( gsqlDialect.getIndexExporter() )
				.isNotNull()
				.isInstanceOf( SpannerIndexExporter.class );
	}

	@Test
	public void testAllVectorSqlTypesDetected() {
		final SpannerIndexExporter exporter = new SpannerIndexExporter( new SpannerDialect() );
		final int[] vectorTypes = {
				SqlTypes.VECTOR,
				SqlTypes.VECTOR_FLOAT32,
				SqlTypes.VECTOR_FLOAT64,
				SqlTypes.VECTOR_INT8,
				SqlTypes.VECTOR_BINARY,
				SqlTypes.VECTOR_FLOAT16
		};
		for ( int typeCode : vectorTypes ) {
			final Table table = new Table( "test", "items" );
			final Column col = new Column( "vec_" + typeCode );
			col.setSqlTypeCode( typeCode );
			final Index idx = table.getOrCreateIndex( "idx_" + typeCode );
			idx.addColumn( col );
			assertThat( exporter.isVectorIndex( idx, null ) )
					.as( "Expected typeCode %d to be detected as vector index", typeCode )
					.isTrue();
		}
	}

	@Test
	public void testOptionsNormalization() {
		final Dialect gsqlDialect = new SpannerDialect();
		final Table table1 = new Table( "test", "docs" );
		final Column col1 = new Column( "v" );
		col1.setSqlTypeCode( SqlTypes.VECTOR_FLOAT32 );
		final Index idx1 = table1.getOrCreateIndex( "idx_gsql" );
		idx1.addColumn( col1 );
		idx1.setOptions( "with (distance_type = 'DOT_PRODUCT')" );

		final String[] gsqlDdl = gsqlDialect.getIndexExporter().getSqlCreateStrings( idx1, null, new TestContext( gsqlDialect ) );
		assertThat( gsqlDdl ).containsExactly(
				"create vector index idx_gsql on docs (v) where v is not null options (distance_type = 'DOT_PRODUCT')"
		);
	}

	@Test
	public void testMultipleVectorAndStandardIndexesTableDrop() {
		final Dialect dialect = new SpannerDialect();
		final Table table = new Table( "test", "multi_index_table" );

		final Column c1 = new Column( "c1" );
		c1.setSqlTypeCode( Types.VARCHAR );
		final Index idxStd1 = table.getOrCreateIndex( "idx_std_1" );
		idxStd1.addColumn( c1 );

		final Column c2 = new Column( "c2" );
		c2.setSqlTypeCode( Types.INTEGER );
		final Index idxStd2 = table.getOrCreateIndex( "idx_std_2" );
		idxStd2.addColumn( c2 );

		final Column v1 = new Column( "v1" );
		v1.setSqlTypeCode( SqlTypes.VECTOR_FLOAT32 );
		final Index idxVec1 = table.getOrCreateIndex( "idx_vec_1" );
		idxVec1.addColumn( v1 );

		final Column v2 = new Column( "v2" );
		v2.setSqlTypeCode( SqlTypes.VECTOR_FLOAT64 );
		final Index idxVec2 = table.getOrCreateIndex( "idx_vec_2" );
		idxVec2.addColumn( v2 );

		final String[] dropDdl = dialect.getTableExporter().getSqlDropStrings( table, null, new TestContext( dialect ) );
		assertThat( dropDdl ).containsExactly(
				"drop index if exists idx_std_1",
				"drop index if exists idx_std_2",
				"drop vector index if exists idx_vec_1",
				"drop vector index if exists idx_vec_2",
				"drop table if exists multi_index_table"
		);
	}

	private record TestContext(Dialect dialect) implements SqlStringGenerationContext {
		@Override
		public Dialect getDialect() {
			return dialect;
		}

		@Override
		public Identifier toIdentifier(String text) {
			return Identifier.toIdentifier( text );
		}

		@Override
		public Identifier getDefaultCatalog() {
			return null;
		}

		@Override
		public Identifier getDefaultSchema() {
			return null;
		}

		@Override
		public String format(QualifiedTableName qualifiedName) {
			return qualifiedName.render();
		}

		@Override
		public String format(QualifiedSequenceName qualifiedName) {
			return qualifiedName.render();
		}

		@Override
		public String format(QualifiedName qualifiedName) {
			return qualifiedName.render();
		}

		@Override
		public String formatWithoutCatalog(QualifiedSequenceName qualifiedName) {
			return qualifiedName.render();
		}

		@Override
		public boolean isMigration() {
			return false;
		}
	}
}
