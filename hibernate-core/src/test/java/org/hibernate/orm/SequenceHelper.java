/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.orm;

import org.hibernate.dialect.SpannerPostgreSQLDialect;
import org.hibernate.testing.orm.junit.SessionFactoryScope;

public class SequenceHelper {
	public static Long getId(SessionFactoryScope scope) {
		return scope.getSessionFactory().getJdbcServices().getDialect() instanceof SpannerPostgreSQLDialect ?
				4611686018427387855L : 1L;
	}
}
