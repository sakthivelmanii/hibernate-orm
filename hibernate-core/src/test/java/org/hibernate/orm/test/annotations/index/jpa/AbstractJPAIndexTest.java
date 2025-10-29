/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.orm.test.annotations.index.jpa;

import org.hibernate.boot.model.naming.ImplicitNamingStrategy;
import org.hibernate.boot.model.naming.ImplicitNamingStrategyJpaCompliantImpl;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.dialect.SpannerPostgreSQLDialect;
import org.hibernate.internal.util.StringHelper;
import org.hibernate.mapping.Bag;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.Index;
import org.hibernate.mapping.Join;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.mapping.Property;
import org.hibernate.mapping.Set;
import org.hibernate.mapping.Table;
import org.hibernate.mapping.UniqueKey;
import org.hibernate.metamodel.CollectionClassification;
import org.hibernate.testing.orm.junit.RequiresDialect;
import org.hibernate.testing.orm.junit.ServiceRegistry;
import org.hibernate.testing.orm.junit.SessionFactory;
import org.hibernate.testing.orm.junit.SessionFactoryScope;
import org.hibernate.testing.orm.junit.SettingProvider;
import org.hibernate.testing.orm.junit.SkipForDialect;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Strong Liu
 */
@SessionFactory
@ServiceRegistry(
		settingProviders = {
				@SettingProvider(settingName = AvailableSettings.DEFAULT_LIST_SEMANTICS,
						provider = AbstractJPAIndexTest.ListSemanticProvider.class),
				@SettingProvider(settingName = AvailableSettings.IMPLICIT_NAMING_STRATEGY,
						provider = AbstractJPAIndexTest.ImplicitNameSettingProvider.class)}
)
public abstract class AbstractJPAIndexTest {

	public static class ImplicitNameSettingProvider implements SettingProvider.Provider<ImplicitNamingStrategy> {
		@Override
		public ImplicitNamingStrategy getSetting() {
			return ImplicitNamingStrategyJpaCompliantImpl.INSTANCE;
		}
	}

	public static class ListSemanticProvider implements SettingProvider.Provider<CollectionClassification> {
		@Override
		public CollectionClassification getSetting() {
			return CollectionClassification.BAG;
		}
	}

	@Test
	@SkipForDialect(dialectClass = SpannerPostgreSQLDialect.class, reason = "Spanner doesn't support"
																			+ "unique constraint definition in the column")
	public void testTableIndex(SessionFactoryScope scope) {
		PersistentClass entity = scope.getMetadataImplementor().getEntityBinding( Car.class.getName() );
		Iterator<UniqueKey> itr = entity.getTable().getUniqueKeys().values().iterator();
		assertThat( itr.hasNext() ).isTrue();
		UniqueKey uk = itr.next();
		assertThat( itr.hasNext() ).isFalse();
		assertThat( StringHelper.isNotEmpty( uk.getName() ) ).isTrue();
		assertThat( uk.getColumnSpan() ).isEqualTo( 2 );
		Column column = uk.getColumns().get( 0 );
		assertThat( column.getName() ).isEqualTo( "brand" );
		column = uk.getColumns().get( 1 );
		assertThat( column.getName() ).isEqualTo( "producer" );
		assertThat( uk.getTable() ).isSameAs( entity.getTable() );

		Iterator<Index> indexItr = entity.getTable().getIndexes().values().iterator();
		assertThat( indexItr.hasNext() ).isTrue();
		Index index = indexItr.next();
		assertThat( indexItr.hasNext() ).isFalse();
		assertThat( index.getName() ).isEqualTo( "Car_idx" );
		assertThat( index.getColumnSpan() ).isEqualTo( 1 );
		column = index.getColumns().iterator().next();
		assertThat( column.getName() ).isEqualTo( "since" );
		assertThat( index.getTable() ).isSameAs( entity.getTable() );
	}

	@Test
	@RequiresDialect(  SpannerPostgreSQLDialect.class )
	public void testTableIndex2(SessionFactoryScope scope) {
		// no unique keys in the table since spanner supports only unique indexes
		PersistentClass entity = scope.getMetadataImplementor().getEntityBinding( Car.class.getName() );
		Iterator itr = entity.getTable().getUniqueKeys().values().iterator();
		assertFalse( itr.hasNext() );

		itr = entity.getTable().getIndexes().values().iterator();

		assertTrue( itr.hasNext() );
		Index index = (Index)itr.next();
		assertTrue( index.isUnique() );
		assertEquals( 2, index.getColumnSpan() );
		List<String> columns = index.getColumns().stream()
				.map( Column::getName ).toList();
		assertThat(columns).hasSize( 2 )
				.containsExactlyInAnyOrderElementsOf( List.of("brand", "producer" ));
		assertSame( entity.getTable(), index.getTable() );

		assertTrue( itr.hasNext() );
		index = (Index)itr.next();
		assertFalse( itr.hasNext() );
		assertEquals( "Car_idx", index.getName() );
		assertEquals( 1, index.getColumnSpan() );

		Column column = index.getColumns().iterator().next();
		assertEquals( "since", column.getName() );
		assertSame( entity.getTable(), index.getTable() );
	}

	@Test
	public void testSecondaryTableIndex(SessionFactoryScope scope) {
		PersistentClass entity = scope.getMetadataImplementor().getEntityBinding( Car.class.getName() );

		Join join = entity.getJoins().get( 0 );
		Iterator<Index> itr = join.getTable().getIndexes().values().iterator();
		assertThat( itr.hasNext() ).isTrue();
		Index index = itr.next();
		assertThat( itr.hasNext() ).isFalse();
		assertThat( StringHelper.isNotEmpty( index.getName() ) )
				.describedAs( "index name is not generated" )
				.isTrue();
		assertThat( index.getColumnSpan() ).isEqualTo( 2 );
		Iterator<Column> columnIterator = index.getColumns().iterator();
		Column column = columnIterator.next();
		assertThat( column.getName() ).isEqualTo( "dealer_name" );
		column = columnIterator.next();
		assertThat( column.getName() ).isEqualTo( "rate" );
		assertThat( index.getTable() ).isSameAs( join.getTable() );

	}

	@Test
	public void testCollectionTableIndex(SessionFactoryScope scope) {
		PersistentClass entity = scope.getMetadataImplementor().getEntityBinding( Car.class.getName() );
		Property property = entity.getProperty( "otherDealers" );
		Set set = (Set) property.getValue();
		Table collectionTable = set.getCollectionTable();

		Iterator<Index> itr = collectionTable.getIndexes().values().iterator();
		assertThat( itr.hasNext() ).isTrue();
		Index index = itr.next();
		assertThat( itr.hasNext() ).isFalse();
		assertThat( StringHelper.isNotEmpty( index.getName() ) )
				.describedAs( "index name is not generated" )
				.isTrue();
		assertThat( index.getColumnSpan() ).isEqualTo( 1 );
		Iterator<Column> columnIterator = index.getColumns().iterator();
		Column column = columnIterator.next();
		assertThat( column.getName() ).isEqualTo( "name" );
		assertThat( index.getTable() ).isSameAs( collectionTable );

	}

	@Test
	public void testJoinTableIndex(SessionFactoryScope scope) {
		PersistentClass entity = scope.getMetadataImplementor().getEntityBinding( Importer.class.getName() );
		Property property = entity.getProperty( "cars" );
		Bag set = (Bag) property.getValue();
		Table collectionTable = set.getCollectionTable();

		Iterator<Index> itr = collectionTable.getIndexes().values().iterator();
		assertThat( itr.hasNext() ).isTrue();
		Index index = itr.next();
		assertThat( itr.hasNext() ).isFalse();
		assertThat( StringHelper.isNotEmpty( index.getName() ) )
				.describedAs( "index name is not generated" )
				.isTrue();
		assertThat( index.getColumnSpan() ).isEqualTo( 1 );
		Iterator<Column> columnIterator = index.getColumns().iterator();
		Column column = columnIterator.next();
		assertThat( column.getName() ).isEqualTo( "importers_id" );
		assertThat( index.getTable() ).isSameAs( collectionTable );
	}

	@Test
	public void testTableGeneratorIndex() {
		//todo
	}
}
