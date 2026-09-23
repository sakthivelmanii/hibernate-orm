/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.vector.internal;

import org.hibernate.dialect.Dialect;
import org.hibernate.sql.spi.SqlAppender;
import org.hibernate.type.descriptor.WrapperOptions;
import org.hibernate.type.descriptor.java.JavaType;
import org.hibernate.type.descriptor.jdbc.JdbcLiteralFormatter;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class SpannerPostgreSQLJdbcLiteralFormatterVectorTest {

	// A simple dummy appender to capture everything appended
	private static class DummyAppender implements SqlAppender {
		private final StringBuilder sb = new StringBuilder();

		@Override
		public void appendSql(String fragment) {
			sb.append(fragment);
		}

		@Override
		public void appendSql(char fragment) {
			sb.append(fragment);
		}

		public String getResult() {
			return sb.toString();
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testFormatFloatArray() {
		JavaType<float[]> mockJavaType = mock(JavaType.class);
		doReturn(float[].class).when(mockJavaType).getJavaTypeClass();

		Object[] unwrapped = new Object[] { 1.0f, 2.0f, 3.0f };
		doReturn(unwrapped).when(mockJavaType).unwrap(any(), eq(Object[].class), any());

		JdbcLiteralFormatter<Object> elementFormatter = mock(JdbcLiteralFormatter.class);
		doAnswer(invocation -> {
			SqlAppender appender = invocation.getArgument(0);
			Object val = invocation.getArgument(1);
			appender.appendSql(val.toString());
			return null;
		}).when(elementFormatter).appendJdbcLiteral(any(), any(), any(), any());

		SpannerPostgreSQLJdbcLiteralFormatterVector<float[]> formatter = new SpannerPostgreSQLJdbcLiteralFormatterVector<>(mockJavaType, elementFormatter, "float4");

		DummyAppender dummyAppender = new DummyAppender();
		formatter.appendJdbcLiteral(dummyAppender, new float[]{1.0f, 2.0f, 3.0f}, mock(Dialect.class), mock(WrapperOptions.class));

		assertEquals("ARRAY[1.0,2.0,3.0]::float4[]", dummyAppender.getResult());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testFormatDoubleArray() {
		JavaType<double[]> mockJavaType = mock(JavaType.class);
		doReturn(double[].class).when(mockJavaType).getJavaTypeClass();

		Object[] unwrapped = new Object[] { 1.0d, 2.0d, 3.0d };
		doReturn(unwrapped).when(mockJavaType).unwrap(any(), eq(Object[].class), any());

		JdbcLiteralFormatter<Object> elementFormatter = mock(JdbcLiteralFormatter.class);
		doAnswer(invocation -> {
			SqlAppender appender = invocation.getArgument(0);
			Object val = invocation.getArgument(1);
			appender.appendSql(val.toString());
			return null;
		}).when(elementFormatter).appendJdbcLiteral(any(), any(), any(), any());

		SpannerPostgreSQLJdbcLiteralFormatterVector<double[]> formatter = new SpannerPostgreSQLJdbcLiteralFormatterVector<>(mockJavaType, elementFormatter, "float8");

		DummyAppender dummyAppender = new DummyAppender();
		formatter.appendJdbcLiteral(dummyAppender, new double[]{1.0d, 2.0d, 3.0d}, mock(Dialect.class), mock(WrapperOptions.class));

		assertEquals("ARRAY[1.0,2.0,3.0]::float8[]", dummyAppender.getResult());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testFormatNull() {
		JavaType<float[]> mockJavaType = mock(JavaType.class);
		doReturn(float[].class).when(mockJavaType).getJavaTypeClass();

		JdbcLiteralFormatter<Object> elementFormatter = mock(JdbcLiteralFormatter.class);
		SpannerPostgreSQLJdbcLiteralFormatterVector<float[]> formatter = new SpannerPostgreSQLJdbcLiteralFormatterVector<>(mockJavaType, elementFormatter, "float4");

		DummyAppender dummyAppender = new DummyAppender();
		formatter.appendJdbcLiteral(dummyAppender, null, mock(Dialect.class), mock(WrapperOptions.class));

		assertEquals("null", dummyAppender.getResult());
	}
}
