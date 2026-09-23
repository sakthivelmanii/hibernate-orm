/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.vector.internal;

import org.hibernate.dialect.Dialect;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.metamodel.mapping.JdbcMapping;
import org.hibernate.sql.spi.SqlAppender;
import org.hibernate.type.BasicType;
import org.hibernate.type.SqlTypes;
import org.hibernate.type.descriptor.ValueExtractor;
import org.hibernate.type.descriptor.WrapperOptions;
import org.hibernate.type.descriptor.java.ArrayJavaType;
import org.hibernate.type.descriptor.java.DoubleJavaType;
import org.hibernate.type.descriptor.java.DoublePrimitiveArrayJavaType;
import org.hibernate.type.descriptor.java.FloatJavaType;
import org.hibernate.type.descriptor.java.FloatPrimitiveArrayJavaType;
import org.hibernate.type.descriptor.java.JavaType;
import org.hibernate.type.descriptor.jdbc.DoubleJdbcType;
import org.hibernate.type.descriptor.jdbc.FloatJdbcType;
import org.hibernate.type.descriptor.jdbc.JdbcLiteralFormatter;
import org.hibernate.type.descriptor.jdbc.JdbcType;
import org.hibernate.type.spi.TypeConfiguration;
import org.junit.jupiter.api.Test;

import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SpannerPostgreSQLVectorJdbcTypeTest {

	@Test
	@SuppressWarnings("unchecked")
	public void testConstructorsAndMetadata() {
		BasicType<Float> mockBasicType = mock(BasicType.class);
		when(mockBasicType.getJdbcType()).thenReturn(FloatJdbcType.INSTANCE);

		SpannerPostgreSQLVectorJdbcType type1 = new SpannerPostgreSQLVectorJdbcType(
				SqlTypes.VECTOR_FLOAT32,
				"float4",
				mockBasicType
		);
		assertEquals(SqlTypes.VECTOR_FLOAT32, type1.getDefaultSqlTypeCode());
		assertEquals("float4", type1.getElementTypeName(mock(JavaType.class), mock(SharedSessionContractImplementor.class)));
		assertEquals(FloatJdbcType.INSTANCE, type1.getElementJdbcType());

		SpannerPostgreSQLVectorJdbcType type2 = new SpannerPostgreSQLVectorJdbcType(
				FloatJdbcType.INSTANCE,
				SqlTypes.VECTOR,
				"float4"
		);
		assertEquals(SqlTypes.VECTOR, type2.getDefaultSqlTypeCode());
		assertEquals("float4", type2.getElementTypeName(mock(JavaType.class), mock(SharedSessionContractImplementor.class)));
	}

	@Test
	public void testRecommendedJavaType() {
		TypeConfiguration typeConfiguration = new TypeConfiguration();

		SpannerPostgreSQLVectorJdbcType floatType = new SpannerPostgreSQLVectorJdbcType(
				FloatJdbcType.INSTANCE,
				SqlTypes.VECTOR_FLOAT32,
				"float4"
		);
		JavaType<?> recFloat = floatType.getRecommendedJavaType(null, null, typeConfiguration);
		assertEquals(float[].class, recFloat.getJavaTypeClass());

		SpannerPostgreSQLVectorJdbcType doubleType1 = new SpannerPostgreSQLVectorJdbcType(
				DoubleJdbcType.INSTANCE,
				SqlTypes.VECTOR_FLOAT64,
				"float8"
		);
		JavaType<?> recDouble1 = doubleType1.getRecommendedJavaType(null, null, typeConfiguration);
		assertEquals(double[].class, recDouble1.getJavaTypeClass());

		SpannerPostgreSQLVectorJdbcType doubleType2 = new SpannerPostgreSQLVectorJdbcType(
				DoubleJdbcType.INSTANCE,
				SqlTypes.VECTOR_FLOAT64,
				"double precision"
		);
		JavaType<?> recDouble2 = doubleType2.getRecommendedJavaType(null, null, typeConfiguration);
		assertEquals(double[].class, recDouble2.getJavaTypeClass());
	}

	@Test
	public void testCastPatterns() {
		SpannerPostgreSQLVectorJdbcType type = new SpannerPostgreSQLVectorJdbcType(
				FloatJdbcType.INSTANCE,
				SqlTypes.VECTOR_FLOAT32,
				"float4"
		);

		JdbcMapping stringMapping = mock(JdbcMapping.class);
		JdbcType stringJdbcType = mock(JdbcType.class);
		when(stringMapping.getJdbcType()).thenReturn(stringJdbcType);
		when(stringJdbcType.isStringLike()).thenReturn(true);

		assertEquals("cast(to_jsonb(?1) as text)", type.castToPattern(stringMapping, null));
		assertEquals(
				"spanner.float32_array(cast(?1 as jsonb))",
				type.castFromPattern(stringMapping, null)
		);

		JdbcMapping nonStringMapping = mock(JdbcMapping.class);
		JdbcType nonStringJdbcType = mock(JdbcType.class);
		when(nonStringMapping.getJdbcType()).thenReturn(nonStringJdbcType);
		when(nonStringJdbcType.isStringLike()).thenReturn(false);

		assertNull(type.castToPattern(nonStringMapping, null));
		assertNull(type.castFromPattern(nonStringMapping, null));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testGetJdbcLiteralFormatter() {
		SpannerPostgreSQLVectorJdbcType type = new SpannerPostgreSQLVectorJdbcType(
				FloatJdbcType.INSTANCE,
				SqlTypes.VECTOR_FLOAT32,
				"float4"
		);

		JdbcLiteralFormatter<?> formatter1 = type.getJdbcLiteralFormatter(FloatPrimitiveArrayJavaType.INSTANCE);
		assertNotNull(formatter1);
		assertTrue(formatter1 instanceof SpannerPostgreSQLJdbcLiteralFormatterVector);
	}

	@Test
	public void testAppendWriteExpressionAndTyped() {
		SpannerPostgreSQLVectorJdbcType type = new SpannerPostgreSQLVectorJdbcType(
				FloatJdbcType.INSTANCE,
				SqlTypes.VECTOR_FLOAT32,
				"float4"
		);

		SqlAppender mockAppender = mock(SqlAppender.class);
		Dialect mockDialect = mock(Dialect.class);

		type.appendWriteExpression("?1", null, mockAppender, mockDialect);
		verify(mockAppender).appendSql("?1");

		assertFalse(type.isWriteExpressionTyped(mockDialect));
	}

	@Test
	public void testEqualsAndHashCodeAndToString() {
		SpannerPostgreSQLVectorJdbcType type1 = new SpannerPostgreSQLVectorJdbcType(
				FloatJdbcType.INSTANCE,
				SqlTypes.VECTOR_FLOAT32,
				"float4"
		);
		SpannerPostgreSQLVectorJdbcType type2 = new SpannerPostgreSQLVectorJdbcType(
				FloatJdbcType.INSTANCE,
				SqlTypes.VECTOR_FLOAT32,
				"float4"
		);
		SpannerPostgreSQLVectorJdbcType type3 = new SpannerPostgreSQLVectorJdbcType(
				DoubleJdbcType.INSTANCE,
				SqlTypes.VECTOR_FLOAT64,
				"float8"
		);

		assertEquals(type1, type2);
		assertEquals(type1.hashCode(), type2.hashCode());
		assertNotEquals(type1, type3);
		assertNotEquals(type1, "not-a-type");

		assertEquals("SpannerPostgreSQLVectorJdbcType(float4[], " + SqlTypes.VECTOR_FLOAT32 + ")", type1.toString());
	}

	@Test
	public void testExtractionFromSqlArrayAndStrings() throws SQLException {
		SpannerPostgreSQLVectorJdbcType floatType = new SpannerPostgreSQLVectorJdbcType(
				FloatJdbcType.INSTANCE,
				SqlTypes.VECTOR_FLOAT32,
				"float4"
		);
		ValueExtractor<float[]> floatExtractor = floatType.getExtractor(FloatPrimitiveArrayJavaType.INSTANCE);

		ResultSet mockRs = mock(ResultSet.class);
		WrapperOptions mockOptions = mock(WrapperOptions.class);

		// 1. Null
		when(mockRs.getObject(1)).thenReturn(null);
		assertNull(floatExtractor.extract(mockRs, 1, mockOptions));

		// 2. java.sql.Array with Float[]
		Array mockSqlArray = mock(Array.class);
		when(mockSqlArray.getArray()).thenReturn(new Float[]{ 1.0f, 2.0f, 3.0f });
		when(mockRs.getObject(2)).thenReturn(mockSqlArray);
		assertArrayEquals(new float[]{ 1.0f, 2.0f, 3.0f }, floatExtractor.extract(mockRs, 2, mockOptions));

		// 3. String with brackets
		when(mockRs.getObject(3)).thenReturn("[1.0, 2.0, 3.0]");
		assertArrayEquals(new float[]{ 1.0f, 2.0f, 3.0f }, floatExtractor.extract(mockRs, 3, mockOptions));

		// 4. String with braces
		when(mockRs.getObject(4)).thenReturn("{1.0, 2.0, 3.0}");
		assertArrayEquals(new float[]{ 1.0f, 2.0f, 3.0f }, floatExtractor.extract(mockRs, 4, mockOptions));

		// 5. Double type extraction with float8
		SpannerPostgreSQLVectorJdbcType doubleType = new SpannerPostgreSQLVectorJdbcType(
				DoubleJdbcType.INSTANCE,
				SqlTypes.VECTOR_FLOAT64,
				"float8"
		);
		ValueExtractor<double[]> doubleExtractor = doubleType.getExtractor(DoublePrimitiveArrayJavaType.INSTANCE);

		when(mockRs.getObject(5)).thenReturn("{10.5, 20.5}");
		assertArrayEquals(new double[]{ 10.5d, 20.5d }, doubleExtractor.extract(mockRs, 5, mockOptions));

		when(mockRs.getObject(6)).thenReturn("[10.5, 20.5]");
		assertArrayEquals(new double[]{ 10.5d, 20.5d }, doubleExtractor.extract(mockRs, 6, mockOptions));
	}

	@Test
	public void testExtractionArrayConversions() throws SQLException {
		SpannerPostgreSQLVectorJdbcType type = new SpannerPostgreSQLVectorJdbcType(
				FloatJdbcType.INSTANCE,
				SqlTypes.VECTOR_FLOAT32,
				"float4"
		);

		ResultSet mockRs = mock(ResultSet.class);
		WrapperOptions mockOptions = mock(WrapperOptions.class);

		// float[] input to Float[], double[], Double[]
		ValueExtractor<Float[]> boxedFloatExtractor = type.getExtractor(new ArrayJavaType<>(FloatJavaType.INSTANCE));
		ValueExtractor<double[]> doubleExtractor = type.getExtractor(DoublePrimitiveArrayJavaType.INSTANCE);
		ValueExtractor<Double[]> boxedDoubleExtractor = type.getExtractor(new ArrayJavaType<>(DoubleJavaType.INSTANCE));
		ValueExtractor<float[]> primitiveFloatExtractor = type.getExtractor(FloatPrimitiveArrayJavaType.INSTANCE);

		when(mockRs.getObject(1)).thenReturn(new float[]{ 1.0f, 2.0f });
		assertArrayEquals(new Float[]{ 1.0f, 2.0f }, boxedFloatExtractor.extract(mockRs, 1, mockOptions));
		assertArrayEquals(new double[]{ 1.0d, 2.0d }, doubleExtractor.extract(mockRs, 1, mockOptions));
		assertArrayEquals(new Double[]{ 1.0d, 2.0d }, boxedDoubleExtractor.extract(mockRs, 1, mockOptions));

		// Float[] input to float[], double[], Double[]
		when(mockRs.getObject(2)).thenReturn(new Float[]{ 3.0f, null, 5.0f });
		assertArrayEquals(new float[]{ 3.0f, 0.0f, 5.0f }, primitiveFloatExtractor.extract(mockRs, 2, mockOptions));
		assertArrayEquals(new double[]{ 3.0d, 0.0d, 5.0d }, doubleExtractor.extract(mockRs, 2, mockOptions));
		assertArrayEquals(new Double[]{ 3.0d, null, 5.0d }, boxedDoubleExtractor.extract(mockRs, 2, mockOptions));

		// double[] input to Double[], float[], Float[]
		when(mockRs.getObject(3)).thenReturn(new double[]{ 4.5d, 5.5d });
		assertArrayEquals(new Double[]{ 4.5d, 5.5d }, boxedDoubleExtractor.extract(mockRs, 3, mockOptions));
		assertArrayEquals(new float[]{ 4.5f, 5.5f }, primitiveFloatExtractor.extract(mockRs, 3, mockOptions));
		assertArrayEquals(new Float[]{ 4.5f, 5.5f }, boxedFloatExtractor.extract(mockRs, 3, mockOptions));

		// Double[] input to double[], float[], Float[]
		when(mockRs.getObject(4)).thenReturn(new Double[]{ 6.5d, null, 8.5d });
		assertArrayEquals(new double[]{ 6.5d, 0.0d, 8.5d }, doubleExtractor.extract(mockRs, 4, mockOptions));
		assertArrayEquals(new float[]{ 6.5f, 0.0f, 8.5f }, primitiveFloatExtractor.extract(mockRs, 4, mockOptions));
		assertArrayEquals(new Float[]{ 6.5f, null, 8.5f }, boxedFloatExtractor.extract(mockRs, 4, mockOptions));

		// Object[] input
		when(mockRs.getObject(5)).thenReturn(new Object[]{ 7.0f, 8.0f });
		assertArrayEquals(new float[]{ 7.0f, 8.0f }, primitiveFloatExtractor.extract(mockRs, 5, mockOptions));
		assertArrayEquals(new Float[]{ 7.0f, 8.0f }, boxedFloatExtractor.extract(mockRs, 5, mockOptions));
		assertArrayEquals(new double[]{ 7.0d, 8.0d }, doubleExtractor.extract(mockRs, 5, mockOptions));
		assertArrayEquals(new Double[]{ 7.0d, 8.0d }, boxedDoubleExtractor.extract(mockRs, 5, mockOptions));

		// Object[] input with string / nulls
		when(mockRs.getObject(6)).thenReturn(new Object[]{ "1.5", null, 3.5 });
		assertArrayEquals(new Float[]{ 1.5f, null, 3.5f }, boxedFloatExtractor.extract(mockRs, 6, mockOptions));
		assertArrayEquals(new Double[]{ 1.5d, null, 3.5d }, boxedDoubleExtractor.extract(mockRs, 6, mockOptions));

		// Unsupported object type throws IllegalArgumentException
		when(mockRs.getObject(7)).thenReturn(12345);
		assertThrows(IllegalArgumentException.class, () -> primitiveFloatExtractor.extract(mockRs, 7, mockOptions));
	}

	@Test
	public void testCallableStatementExtraction() throws SQLException {
		SpannerPostgreSQLVectorJdbcType type = new SpannerPostgreSQLVectorJdbcType(
				FloatJdbcType.INSTANCE,
				SqlTypes.VECTOR_FLOAT32,
				"float4"
		);
		ValueExtractor<float[]> extractor = type.getExtractor(FloatPrimitiveArrayJavaType.INSTANCE);

		CallableStatement mockStmt = mock(CallableStatement.class);
		WrapperOptions mockOptions = mock(WrapperOptions.class);

		when(mockStmt.getObject(1)).thenReturn(new float[]{ 1.0f, 2.0f });
		when(mockStmt.getObject("vecParam")).thenReturn(new float[]{ 3.0f, 4.0f });

		assertArrayEquals(new float[]{ 1.0f, 2.0f }, extractor.extract(mockStmt, 1, mockOptions));
		assertArrayEquals(new float[]{ 3.0f, 4.0f }, extractor.extract(mockStmt, "vecParam", mockOptions));
	}
}
