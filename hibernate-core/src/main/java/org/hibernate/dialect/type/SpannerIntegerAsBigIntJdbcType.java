/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.dialect.type;

import org.hibernate.type.descriptor.jdbc.BigIntJdbcType;

import java.sql.Types;

public class SpannerIntegerAsBigIntJdbcType extends BigIntJdbcType {

	public static final SpannerIntegerAsBigIntJdbcType INSTANCE = new SpannerIntegerAsBigIntJdbcType();

	@Override
	public int getJdbcTypeCode() {
		return Types.INTEGER;
	}
}
