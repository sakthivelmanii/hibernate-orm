/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.dialect.type;

import org.hibernate.type.descriptor.jdbc.BigIntJdbcType;

import java.sql.Types;

public class SpannerSmallIntAsBigIntJdbcType extends BigIntJdbcType {

	public static final SpannerSmallIntAsBigIntJdbcType INSTANCE = new SpannerSmallIntAsBigIntJdbcType();

	@Override
	public int getJdbcTypeCode() {
		return Types.SMALLINT;
	}
}
