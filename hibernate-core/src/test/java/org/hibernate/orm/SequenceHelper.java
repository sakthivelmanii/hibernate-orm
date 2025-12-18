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
	public static long getId(SessionFactoryScope scope, long index) {
		return getId( scope, 1L, index );
	}

	public static long getId(EntityManagerFactoryScope scope, long n) {
		return getId( scope.getDialect(), 1L, n );
	}

	public static long getId(SessionFactoryScope scope, long startsWith, long index) {
		return getId( scope.getSessionFactory().getJdbcServices().getDialect(), startsWith, index );
	}

	private static long getId(Dialect dialect, long startsWith, long index) {
		return dialect instanceof SpannerPostgreSQLDialect ? (Long.reverse(startsWith) >>> 1) - 50L + index : index;
	}

	public static long getIncrementValue(Dialect dialect, long incrementValue) {
		return dialect instanceof SpannerPostgreSQLDialect ? 0 : incrementValue;
	}

	public static Object getValue(SessionFactoryScope scope, int val) {
		return SpannerHelper.isSpannerDatabase(scope) ? (long) val : val;
	}
}
