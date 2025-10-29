/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.orm.test.bulkid;


import org.hibernate.dialect.SpannerPostgreSQLDialect;
import org.hibernate.testing.orm.junit.SkipForDialect;

/**
 * Special test that tries to update 1100 rows. Oracle only supports up to 1000 parameters per in-predicate,
 * so we want to test if this scenario works.
 *
 * @author Vlad Mihalcea
 */
@SkipForDialect( dialectClass = SpannerPostgreSQLDialect.class, reason = "##DISABLE##")
public class OracleInlineMutationStrategyIdTest extends InlineMutationStrategyIdTest {

	@Override
	protected int entityCount() {
		return 1100;
	}
}
