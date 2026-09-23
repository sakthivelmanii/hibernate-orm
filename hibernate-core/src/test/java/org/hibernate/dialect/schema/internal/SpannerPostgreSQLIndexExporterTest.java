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
import org.hibernate.dialect.SpannerPostgreSQLDialect;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.Index;
import org.hibernate.mapping.Table;
import org.hibernate.testing.orm.junit.BaseUnitTest;
import org.hibernate.type.SqlTypes;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@BaseUnitTest
public class SpannerPostgreSQLIndexExporterTest {

	@Test
	public void testSpannerPostgreSqlVectorIndexCreate() {
		final Dialect dialect = new SpannerPostgreSQLDialect();
		final Table table = new Table( "test", "documents" );
		final Column col = new Column( "embedding" );
		col.setSqlTypeCode( SqlTypes.VECTOR_FLOAT32 );

		final Index index = table.getOrCreateIndex( "idx_doc_embedding" );
		index.addColumn( col );

		final String[] ddl = dialect.getIndexExporter().getSqlCreateStrings( index, null, new TestContext( dialect ) );
		assertThat( ddl ).containsExactly(
				"create index idx_doc_embedding on documents using scann (embedding) with (distance_type = 'COSINE')"
		);
	}

	@Test
	public void testSpannerPostgreSqlVectorIndexCreateCustomOptions() {
		final Dialect dialect = new SpannerPostgreSQLDialect();
		final Table table = new Table( "test", "documents" );
		final Column col = new Column( "embedding" );
		col.setSqlTypeCode( SqlTypes.VECTOR_FLOAT32 );

		final Index index = table.getOrCreateIndex( "idx_doc_embedding" );
		index.addColumn( col );
		index.setOptions( "with (distance_type = 'DOT_PRODUCT')" );

		final String[] ddl = dialect.getIndexExporter().getSqlCreateStrings( index, null, new TestContext( dialect ) );
		assertThat( ddl ).containsExactly(
				"create index idx_doc_embedding on documents using scann (embedding) with (distance_type = 'DOT_PRODUCT')"
		);
	}

	@Test
	public void testSpannerPostgreSqlVectorIndexDrop() {
		final Dialect dialect = new SpannerPostgreSQLDialect();
		final Table table = new Table( "test", "documents" );
		final Column col = new Column( "embedding" );
		col.setSqlTypeCode( SqlTypes.VECTOR_FLOAT32 );

		final Index index = table.getOrCreateIndex( "idx_doc_embedding" );
		index.addColumn( col );

		final String[] ddl = dialect.getIndexExporter().getSqlDropStrings( index, null, new TestContext( dialect ) );
		assertThat( ddl ).containsExactly(
				"drop index if exists idx_doc_embedding"
		);
	}

	@Test
	public void testSpannerPostgreSqlStandardIndexCreateAndDrop() {
		final Dialect dialect = new SpannerPostgreSQLDialect();
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
	public void testVectorIndexDetectedByExplicitUsing() {
		final Dialect dialect = new SpannerPostgreSQLDialect();
		final Table table = new Table( "test", "documents" );
		final Column col = new Column( "embedding" );

		final Index index = table.getOrCreateIndex( "idx_doc_embedding" );
		index.addColumn( col );
		index.setUsing( "scann" );

		final String[] ddl = dialect.getIndexExporter().getSqlCreateStrings( index, null, new TestContext( dialect ) );
		assertThat( ddl ).containsExactly(
				"create index idx_doc_embedding on documents using scann (embedding) with (distance_type = 'COSINE')"
		);
	}

	@Test
	public void testVectorIndexDetectedByExplicitType() {
		final Dialect dialect = new SpannerPostgreSQLDialect();
		final Table table = new Table( "test", "documents" );
		final Column col = new Column( "embedding" );

		final Index index = table.getOrCreateIndex( "idx_doc_embedding" );
		index.addColumn( col );
		index.setType( "vector" );

		final String[] ddl = dialect.getIndexExporter().getSqlCreateStrings( index, null, new TestContext( dialect ) );
		assertThat( ddl ).containsExactly(
				"create index idx_doc_embedding on documents using scann (embedding) with (distance_type = 'COSINE')"
		);
	}

	@Test
	public void testSpannerPostgreSqlTableExporterDropWithVectorIndex() {
		final Dialect dialect = new SpannerPostgreSQLDialect();
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
				"drop index if exists idx_doc_embedding",
				"drop table if exists documents"
		);
	}

	@Test
	public void testDialectExposesSpannerPostgreSQLIndexExporter() {
		final Dialect pgDialect = new SpannerPostgreSQLDialect();
		assertThat( pgDialect.getIndexExporter() )
				.isNotNull()
				.isInstanceOf( SpannerPostgreSQLIndexExporter.class );
	}

	@Test
	public void testOptionsNormalization() {
		final Dialect pgDialect = new SpannerPostgreSQLDialect();

		// Case 1: options keyword translated to with
		final Table table1 = new Table( "test", "docs" );
		final Column col1 = new Column( "v" );
		col1.setSqlTypeCode( SqlTypes.VECTOR_FLOAT32 );
		final Index idx1 = table1.getOrCreateIndex( "idx_1" );
		idx1.addColumn( col1 );
		idx1.setOptions( "options (distance_type = 'EUCLIDEAN')" );

		final String[] ddl1 = pgDialect.getIndexExporter().getSqlCreateStrings( idx1, null, new TestContext( pgDialect ) );
		assertThat( ddl1 ).containsExactly(
				"create index idx_1 on docs using scann (v) with (distance_type = 'EUCLIDEAN')"
		);

		// Case 2: parentheses only
		final Table table2 = new Table( "test", "docs" );
		final Column col2 = new Column( "v" );
		col2.setSqlTypeCode( SqlTypes.VECTOR_FLOAT32 );
		final Index idx2 = table2.getOrCreateIndex( "idx_2" );
		idx2.addColumn( col2 );
		idx2.setOptions( "(distance_type = 'DOT_PRODUCT')" );

		final String[] ddl2 = pgDialect.getIndexExporter().getSqlCreateStrings( idx2, null, new TestContext( pgDialect ) );
		assertThat( ddl2 ).containsExactly(
				"create index idx_2 on docs using scann (v) with (distance_type = 'DOT_PRODUCT')"
		);

		// Case 3: raw string
		final Table table3 = new Table( "test", "docs" );
		final Column col3 = new Column( "v" );
		col3.setSqlTypeCode( SqlTypes.VECTOR_FLOAT32 );
		final Index idx3 = table3.getOrCreateIndex( "idx_3" );
		idx3.addColumn( col3 );
		idx3.setOptions( "distance_type = 'COSINE'" );

		final String[] ddl3 = pgDialect.getIndexExporter().getSqlCreateStrings( idx3, null, new TestContext( pgDialect ) );
		assertThat( ddl3 ).containsExactly(
				"create index idx_3 on docs using scann (v) with (distance_type = 'COSINE')"
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
