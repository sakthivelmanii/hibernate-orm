/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.orm;

import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.SpannerPostgreSQLDialect;
import org.hibernate.testing.orm.junit.EntityManagerFactoryScope;
import org.hibernate.testing.orm.junit.SessionFactoryScope;

public class SequenceHelper {

	public static long getId(SessionFactoryScope scope, long startWith) {
		return getId( scope.getSessionFactory().getJdbcServices().getDialect(), startWith );
	}

	public static long getId(EntityManagerFactoryScope scope, long startWith) {
		return getId( scope.getDialect(), startWith);
	}

	public static long getId(Dialect dialect, long startsWith) {
		return dialect instanceof SpannerPostgreSQLDialect ? Long.reverse(startsWith) >>> 1 : startsWith;
	}


	public static long getIncrementValue(Dialect dialect, long incrementValue) {
		return dialect instanceof SpannerPostgreSQLDialect ? 0 : incrementValue;
	}

	public static Object getValue(SessionFactoryScope scope, int val) {
		return SpannerHelper.isSpannerDatabase(scope) ? (long) val : val;
	}

	public static Object getValue(SessionFactoryScope scope, char val) {
		return SpannerHelper.isSpannerDatabase(scope) ? String.valueOf(val) : val;
	}
}
