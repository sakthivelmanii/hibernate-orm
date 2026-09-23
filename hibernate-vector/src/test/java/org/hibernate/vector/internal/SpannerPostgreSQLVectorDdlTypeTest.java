/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.vector.internal;

import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.Size;
import org.hibernate.type.SqlTypes;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

public class SpannerPostgreSQLVectorDdlTypeTest {

	@Test
	public void testGetTypeNameWithDimension() {
		Dialect mockDialect = mock(Dialect.class);
		SpannerPostgreSQLVectorDdlType ddlType = new SpannerPostgreSQLVectorDdlType(SqlTypes.VECTOR, "float4", mockDialect);

		Size size = new Size();
		size.setArrayLength(128);

		String typeName = ddlType.getTypeName(size, null, null);
		assertEquals("float4[] VECTOR LENGTH 128", typeName);
	}

	@Test
	public void testGetTypeNameUnconstrained() {
		Dialect mockDialect = mock(Dialect.class);
		SpannerPostgreSQLVectorDdlType ddlType = new SpannerPostgreSQLVectorDdlType(SqlTypes.VECTOR, "float4", mockDialect);

		Size size = new Size(); // No array length

		String typeName = ddlType.getTypeName(size, null, null);
		assertEquals("float4[]", typeName);

		size.setArrayLength(0); // Zero length
		typeName = ddlType.getTypeName(size, null, null);
		assertEquals("float4[]", typeName);

		size.setArrayLength(-1); // Negative length
		typeName = ddlType.getTypeName(size, null, null);
		assertEquals("float4[]", typeName);
	}

	@Test
	public void testGetCastTypeName() {
		Dialect mockDialect = mock(Dialect.class);
		SpannerPostgreSQLVectorDdlType ddlType = new SpannerPostgreSQLVectorDdlType(SqlTypes.VECTOR, "float4", mockDialect);

		Size size = new Size();
		size.setArrayLength(128);

		String castName = ddlType.getCastTypeName(size, null, null);
		assertEquals("float4[]", castName);
	}
}
