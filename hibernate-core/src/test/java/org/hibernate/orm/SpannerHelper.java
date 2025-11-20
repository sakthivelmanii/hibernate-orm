/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.orm;

import org.hibernate.dialect.SpannerPostgreSQLDialect;
import org.hibernate.testing.orm.junit.SessionFactoryScope;

public class SpannerHelper {
	public static boolean isSpannerDatabase(SessionFactoryScope sessionFactoryScope) {
		return sessionFactoryScope.getSessionFactory().getJdbcServices().getDialect()
				instanceof SpannerPostgreSQLDialect;
	}
}
