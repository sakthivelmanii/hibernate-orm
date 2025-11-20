/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.orm;

import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.SpannerPostgreSQLDialect;
import org.hibernate.testing.orm.junit.DomainModelScope;
import org.hibernate.testing.orm.junit.SessionFactoryScope;

public class SpannerHelper {
	public static boolean isSpannerDatabase(SessionFactoryScope sessionFactoryScope) {
		return isSpannerDatabase( sessionFactoryScope.getSessionFactory().getJdbcServices().getDialect() );
	}

	public static boolean isSpannerDatabase(DomainModelScope scope) {
		return isSpannerDatabase( scope.getDomainModel().getDatabase().getDialect() );
	}

	private static boolean isSpannerDatabase(Dialect dialect) {
		return dialect instanceof SpannerPostgreSQLDialect;
	}
}
