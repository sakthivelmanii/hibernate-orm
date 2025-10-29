/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.dialect.type;

import org.hibernate.type.descriptor.jdbc.BigIntJdbcType;

import java.sql.Types;

public class SpannerIntegerAsBigIntType extends BigIntJdbcType {

	public static final SpannerIntegerAsBigIntType INSTANCE = new SpannerIntegerAsBigIntType();

	@Override
	public int getJdbcTypeCode() {
		return Types.INTEGER;
	}
}
