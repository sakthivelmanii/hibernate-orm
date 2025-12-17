/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.orm.test.annotations.various;

import org.hibernate.dialect.SpannerPostgreSQLDialect;
import org.hibernate.testing.orm.junit.DomainModel;
import org.hibernate.testing.orm.junit.SessionFactory;
import org.hibernate.testing.orm.junit.SessionFactoryScope;
import org.hibernate.testing.orm.junit.SkipForDialect;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Test for the @Timestamp annotation.
 *
 * @author Hardy Ferentschik
 */
@SessionFactory
@DomainModel(annotatedClasses = {VMTimestamped.class, DBTimestamped.class})
@SkipForDialect(dialectClass = SpannerPostgreSQLDialect.class, reason = "Spanner doesn't support integer sequences")
public class TimestampTest {
	@Test void test(SessionFactoryScope scope) {
		scope.inTransaction( session -> {
			var vmTimestamped = new VMTimestamped();
			session.persist( vmTimestamped );
			var dbTimestamped = new DBTimestamped();
			session.persist( dbTimestamped );
			session.flush();
			assertNotNull( vmTimestamped.getLastUpdate() );
			assertNotNull( dbTimestamped.getLastUpdate() );
			assertInstanceOf( Date.class, vmTimestamped.getLastUpdate() );
			assertInstanceOf( Timestamp.class, dbTimestamped.getLastUpdate() );
		} );
		scope.inTransaction( session -> {
			var vmTimestamped = session.find(VMTimestamped.class, 1);
			var dbTimestamped = session.find(DBTimestamped.class, 1);
			assertNotNull( vmTimestamped.getLastUpdate() );
			assertNotNull( dbTimestamped.getLastUpdate() );
			assertInstanceOf( Timestamp.class, vmTimestamped.getLastUpdate() );
			assertInstanceOf( Timestamp.class, dbTimestamped.getLastUpdate() );
		} );

	}
}
