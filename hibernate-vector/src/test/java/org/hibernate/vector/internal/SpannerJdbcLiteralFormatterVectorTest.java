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

public class SpannerJdbcLiteralFormatterVectorTest {

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

		SpannerJdbcLiteralFormatterVector<float[]> formatter = new SpannerJdbcLiteralFormatterVector<>(mockJavaType, elementFormatter, "FLOAT32");

		DummyAppender dummyAppender = new DummyAppender();
		formatter.appendJdbcLiteral(dummyAppender, new float[]{1.0f, 2.0f, 3.0f}, mock(Dialect.class), mock(WrapperOptions.class));

		assertEquals("ARRAY<FLOAT32>[1.0,2.0,3.0]", dummyAppender.getResult());
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

		SpannerJdbcLiteralFormatterVector<double[]> formatter = new SpannerJdbcLiteralFormatterVector<>(mockJavaType, elementFormatter, "FLOAT64");

		DummyAppender dummyAppender = new DummyAppender();
		formatter.appendJdbcLiteral(dummyAppender, new double[]{1.0d, 2.0d, 3.0d}, mock(Dialect.class), mock(WrapperOptions.class));

		assertEquals("ARRAY<FLOAT64>[1.0,2.0,3.0]", dummyAppender.getResult());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testFormatNull() {
		JavaType<float[]> mockJavaType = mock(JavaType.class);
		doReturn(float[].class).when(mockJavaType).getJavaTypeClass();

		JdbcLiteralFormatter<Object> elementFormatter = mock(JdbcLiteralFormatter.class);
		SpannerJdbcLiteralFormatterVector<float[]> formatter = new SpannerJdbcLiteralFormatterVector<>(mockJavaType, elementFormatter, "FLOAT32");

		DummyAppender dummyAppender = new DummyAppender();
		formatter.appendJdbcLiteral(dummyAppender, null, mock(Dialect.class), mock(WrapperOptions.class));

		assertEquals("null", dummyAppender.getResult());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testFormatEmptyArray() {
		JavaType<float[]> mockJavaType = mock(JavaType.class);
		doReturn(float[].class).when(mockJavaType).getJavaTypeClass();

		Object[] unwrapped = new Object[] {};
		doReturn(unwrapped).when(mockJavaType).unwrap(any(), eq(Object[].class), any());

		JdbcLiteralFormatter<Object> elementFormatter = mock(JdbcLiteralFormatter.class);
		SpannerJdbcLiteralFormatterVector<float[]> formatter = new SpannerJdbcLiteralFormatterVector<>(mockJavaType, elementFormatter, "FLOAT32");

		DummyAppender dummyAppender = new DummyAppender();
		formatter.appendJdbcLiteral(dummyAppender, new float[]{}, mock(Dialect.class), mock(WrapperOptions.class));

		assertEquals("ARRAY<FLOAT32>[]", dummyAppender.getResult());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testFormatSingleElementArray() {
		JavaType<float[]> mockJavaType = mock(JavaType.class);
		doReturn(float[].class).when(mockJavaType).getJavaTypeClass();

		Object[] unwrapped = new Object[] { 42.0f };
		doReturn(unwrapped).when(mockJavaType).unwrap(any(), eq(Object[].class), any());

		JdbcLiteralFormatter<Object> elementFormatter = mock(JdbcLiteralFormatter.class);
		doAnswer(invocation -> {
			SqlAppender appender = invocation.getArgument(0);
			Object val = invocation.getArgument(1);
			appender.appendSql(val.toString());
			return null;
		}).when(elementFormatter).appendJdbcLiteral(any(), any(), any(), any());

		SpannerJdbcLiteralFormatterVector<float[]> formatter = new SpannerJdbcLiteralFormatterVector<>(mockJavaType, elementFormatter, "FLOAT32");

		DummyAppender dummyAppender = new DummyAppender();
		formatter.appendJdbcLiteral(dummyAppender, new float[]{42.0f}, mock(Dialect.class), mock(WrapperOptions.class));

		assertEquals("ARRAY<FLOAT32>[42.0]", dummyAppender.getResult());
	}
}
