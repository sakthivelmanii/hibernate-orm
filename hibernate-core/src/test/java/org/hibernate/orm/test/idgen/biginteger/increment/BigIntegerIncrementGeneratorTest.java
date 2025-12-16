/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.orm.test.idgen.biginteger.increment;

import org.hibernate.dialect.SpannerPostgreSQLDialect;
import org.hibernate.testing.orm.junit.DomainModel;
import org.hibernate.testing.orm.junit.SessionFactory;
import org.hibernate.testing.orm.junit.SessionFactoryScope;
import org.hibernate.testing.orm.junit.SkipForDialect;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SuppressWarnings("JUnitMalformedDeclaration")
@DomainModel(xmlMappings = "org/hibernate/orm/test/idgen/biginteger/increment/Mapping.hbm.xml")
@SessionFactory
@SkipForDialect(dialectClass = SpannerPostgreSQLDialect.class, reason = "Spanner doesn't support PG.NUMERIC as primary key")
public class BigIntegerIncrementGeneratorTest {
	@Test
	public void testBasics(SessionFactoryScope scope) {
		scope.inTransaction(
				(session) -> {
					Entity entity = new Entity( "BigInteger + increment #1" );
					session.persist( entity );
					Entity entity2 = new Entity( "BigInteger + increment #2" );
					session.persist( entity2 );

					session.flush();

					assertEquals( BigInteger.valueOf( 1 ), entity.getId() );
					assertEquals( BigInteger.valueOf( 2 ), entity2.getId() );
				}
		);
	}

	@AfterEach
	public void dropTestData(SessionFactoryScope scope) {
		scope.dropData();
	}
}
