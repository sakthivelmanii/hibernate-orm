/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.vector.internal;

import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.Size;
import org.hibernate.type.SqlTypes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

/**
 * Fast, pure unit tests for {@link SpannerVectorDdlType}, verifying GoogleSQL DDL rendering
 * for unconstrained, dimensioned, and quantized vector types (SFP8, SFP4).
 */
public class SpannerVectorDdlTypeTest {

	private Dialect mockDialect;

	@BeforeEach
	public void setUp() {
		mockDialect = mock( Dialect.class );
	}

	// =========================================================================
	// 1. Unconstrained Vectors (No dimension, no quantization)
	// =========================================================================

	@Test
	@DisplayName("Unconstrained vector column renders clean ARRAY<FLOAT32>")
	public void testGetTypeNameUnconstrained() {
		SpannerVectorDdlType ddlType = new SpannerVectorDdlType( SqlTypes.VECTOR, "FLOAT32", mockDialect );

		Size size = new Size(); // No array length set
		assertEquals( "ARRAY<FLOAT32>", ddlType.getTypeName( size, null, null ) );

		size.setArrayLength( 0 ); // Zero length
		assertEquals( "ARRAY<FLOAT32>", ddlType.getTypeName( size, null, null ) );

		size.setArrayLength( -1 ); // Negative length
		assertEquals( "ARRAY<FLOAT32>", ddlType.getTypeName( size, null, null ) );
	}

	@Test
	@DisplayName("Null Size parameter does not throw NPE and renders ARRAY<FLOAT32>")
	public void testGetTypeNameNullSize() {
		SpannerVectorDdlType ddlType = new SpannerVectorDdlType( SqlTypes.VECTOR, "FLOAT32", mockDialect );
		assertEquals( "ARRAY<FLOAT32>", ddlType.getTypeName( null, null, null ) );
		assertEquals( "ARRAY<FLOAT32>", ddlType.getCastTypeName( null, null, null ) );
	}

	// =========================================================================
	// 2. Dimension Only Vectors (No quantization)
	// =========================================================================

	@Test
	@DisplayName("Dimensioned vector renders ARRAY<FLOAT32>(vector_length=>N)")
	public void testGetTypeNameWithDimensionOnly() {
		SpannerVectorDdlType ddlType = new SpannerVectorDdlType( SqlTypes.VECTOR, "FLOAT32", mockDialect );

		Size size = new Size();
		size.setArrayLength( 128 );
		assertEquals( "ARRAY<FLOAT32>(vector_length=>128)", ddlType.getTypeName( size, null, null ) );

		// Large dimension (e.g. OpenAI / Gecko text-embedding-004)
		Size largeSize = new Size();
		largeSize.setArrayLength( 1536 );
		assertEquals( "ARRAY<FLOAT32>(vector_length=>1536)", ddlType.getTypeName( largeSize, null, null ) );
	}

	// =========================================================================
	// 3. SFP8 Quantization Format (8-bit Scalar Float Point)
	// =========================================================================

	@Test
	@DisplayName("Dimensioned vector with SFP8 renders ARRAY<FLOAT32>(vector_length=>128, quantization_format=>'SFP8')")
	public void testGetTypeNameWithQuantizationFormatSfp8() {
		SpannerVectorDdlType ddlType = new SpannerVectorDdlType( SqlTypes.VECTOR, "FLOAT32", "SFP8", mockDialect );

		Size size = new Size();
		size.setArrayLength( 128 );

		String typeName = ddlType.getTypeName( size, null, null );
		assertEquals( "ARRAY<FLOAT32>(vector_length=>128, quantization_format=>'SFP8')", typeName );
		assertEquals( "SFP8", ddlType.getQuantizationFormat() );
	}

	@Test
	@DisplayName("SFP8 format with case-insensitivity, surrounding quotes, and whitespace normalization")
	public void testGetTypeNameSfp8Normalization() {
		// Lowercase "sfp8" should normalize to "SFP8"
		SpannerVectorDdlType lowerDdl = new SpannerVectorDdlType( SqlTypes.VECTOR, "FLOAT32", "sfp8", mockDialect );
		Size size = new Size();
		size.setArrayLength( 256 );
		assertEquals( "ARRAY<FLOAT32>(vector_length=>256, quantization_format=>'SFP8')", lowerDdl.getTypeName( size, null, null ) );
		assertEquals( "SFP8", lowerDdl.getQuantizationFormat() );

		// Surrounding whitespace "  SFP8  " should normalize to "SFP8"
		SpannerVectorDdlType wsDdl = new SpannerVectorDdlType( SqlTypes.VECTOR, "FLOAT32", "  SFP8  ", mockDialect );
		assertEquals( "ARRAY<FLOAT32>(vector_length=>256, quantization_format=>'SFP8')", wsDdl.getTypeName( size, null, null ) );
		assertEquals( "SFP8", wsDdl.getQuantizationFormat() );

		// Single quoted "'SFP8'"
		SpannerVectorDdlType quotedDdl = new SpannerVectorDdlType( SqlTypes.VECTOR, "FLOAT32", "'SFP8'", mockDialect );
		assertEquals( "ARRAY<FLOAT32>(vector_length=>256, quantization_format=>'SFP8')", quotedDdl.getTypeName( size, null, null ) );
		assertEquals( "SFP8", quotedDdl.getQuantizationFormat() );
	}

	// =========================================================================
	// 4. SFP4 Quantization Format (4-bit Scalar Float Point)
	// =========================================================================

	@Test
	@DisplayName("Dimensioned vector with SFP4 renders ARRAY<FLOAT32>(vector_length=>128, quantization_format=>'SFP4')")
	public void testGetTypeNameWithQuantizationFormatSfp4() {
		SpannerVectorDdlType ddlType = new SpannerVectorDdlType( SqlTypes.VECTOR, "FLOAT32", "SFP4", mockDialect );

		Size size = new Size();
		size.setArrayLength( 128 );

		String typeName = ddlType.getTypeName( size, null, null );
		assertEquals( "ARRAY<FLOAT32>(vector_length=>128, quantization_format=>'SFP4')", typeName );
		assertEquals( "SFP4", ddlType.getQuantizationFormat() );
	}

	@Test
	@DisplayName("SFP4 format with lowercase normalization")
	public void testGetTypeNameSfp4Normalization() {
		SpannerVectorDdlType lowerDdl = new SpannerVectorDdlType( SqlTypes.VECTOR, "FLOAT32", "sfp4", mockDialect );
		Size size = new Size();
		size.setArrayLength( 512 );
		assertEquals( "ARRAY<FLOAT32>(vector_length=>512, quantization_format=>'SFP4')", lowerDdl.getTypeName( size, null, null ) );
		assertEquals( "SFP4", lowerDdl.getQuantizationFormat() );
	}

	// =========================================================================
	// 5. Quantization Without Dimension (Unconstrained length with quantization)
	// =========================================================================

	@Test
	@DisplayName("Quantized vector without dimension renders ARRAY<FLOAT32>(quantization_format=>'SFP8')")
	public void testGetTypeNameQuantizationWithoutDimension() {
		SpannerVectorDdlType ddlTypeSfp8 = new SpannerVectorDdlType( SqlTypes.VECTOR, "FLOAT32", "SFP8", mockDialect );
		assertEquals( "ARRAY<FLOAT32>(quantization_format=>'SFP8')", ddlTypeSfp8.getTypeName( null, null, null ) );

		Size unconstrainedSize = new Size();
		assertEquals( "ARRAY<FLOAT32>(quantization_format=>'SFP8')", ddlTypeSfp8.getTypeName( unconstrainedSize, null, null ) );

		SpannerVectorDdlType ddlTypeSfp4 = new SpannerVectorDdlType( SqlTypes.VECTOR, "FLOAT32", "SFP4", mockDialect );
		assertEquals( "ARRAY<FLOAT32>(quantization_format=>'SFP4')", ddlTypeSfp4.getTypeName( null, null, null ) );
	}

	// =========================================================================
	// 6. Dynamic JPA @Column(precision) and @Column(scale) Mapping
	// =========================================================================

	@Test
	@DisplayName("Resolves SFP8 and SFP4 dynamically from Size precision")
	public void testQuantizationViaPrecision() {
		SpannerVectorDdlType ddlType = new SpannerVectorDdlType( SqlTypes.VECTOR, "FLOAT32", mockDialect );

		Size size8 = new Size();
		size8.setArrayLength( 128 );
		size8.setPrecision( 8 );
		assertEquals( "ARRAY<FLOAT32>(vector_length=>128, quantization_format=>'SFP8')", ddlType.getTypeName( size8, null, null ) );

		Size size4 = new Size();
		size4.setArrayLength( 128 );
		size4.setPrecision( 4 );
		assertEquals( "ARRAY<FLOAT32>(vector_length=>128, quantization_format=>'SFP4')", ddlType.getTypeName( size4, null, null ) );

		// Precision overrides instance-configured format
		SpannerVectorDdlType ddlTypeSfp8 = new SpannerVectorDdlType( SqlTypes.VECTOR, "FLOAT32", "SFP8", mockDialect );
		assertEquals( "ARRAY<FLOAT32>(vector_length=>128, quantization_format=>'SFP4')", ddlTypeSfp8.getTypeName( size4, null, null ) );
	}

	@Test
	@DisplayName("Resolves SFP8 and SFP4 dynamically from Size scale as fallback")
	public void testQuantizationViaScale() {
		SpannerVectorDdlType ddlType = new SpannerVectorDdlType( SqlTypes.VECTOR, "FLOAT32", mockDialect );

		Size scale8 = new Size();
		scale8.setArrayLength( 128 );
		scale8.setScale( 8 );
		assertEquals( "ARRAY<FLOAT32>(vector_length=>128, quantization_format=>'SFP8')", ddlType.getTypeName( scale8, null, null ) );

		Size scale4 = new Size();
		scale4.setArrayLength( 128 );
		scale4.setScale( 4 );
		assertEquals( "ARRAY<FLOAT32>(vector_length=>128, quantization_format=>'SFP4')", ddlType.getTypeName( scale4, null, null ) );
	}

	// =========================================================================
	// 7. Programmatic getTypeName(Size, String) Overload
	// =========================================================================

	@Test
	@DisplayName("Programmatic getTypeName overload accepts explicit quantization format")
	public void testProgrammaticGetTypeNameOverload() {
		SpannerVectorDdlType ddlType = new SpannerVectorDdlType( SqlTypes.VECTOR, "FLOAT32", mockDialect );

		Size size = new Size();
		size.setArrayLength( 128 );

		assertEquals( "ARRAY<FLOAT32>(vector_length=>128, quantization_format=>'SFP8')", ddlType.getTypeName( size, "SFP8" ) );
		assertEquals( "ARRAY<FLOAT32>(vector_length=>128, quantization_format=>'SFP4')", ddlType.getTypeName( size, "sfp4" ) );
		assertEquals( "ARRAY<FLOAT32>(vector_length=>128)", ddlType.getTypeName( size, (String) null ) );
	}

	// =========================================================================
	// 8. Regression Verification for Non-Quantized Vectors
	// =========================================================================

	@Test
	@DisplayName("Null and blank quantization format defaults cleanly to non-quantized behavior")
	public void testNullAndBlankQuantizationFormatDefaults() {
		SpannerVectorDdlType nullQuant = new SpannerVectorDdlType( SqlTypes.VECTOR, "FLOAT32", null, mockDialect );
		assertNull( nullQuant.getQuantizationFormat() );

		Size size = new Size();
		size.setArrayLength( 128 );
		assertEquals( "ARRAY<FLOAT32>(vector_length=>128)", nullQuant.getTypeName( size, null, null ) );

		SpannerVectorDdlType blankQuant = new SpannerVectorDdlType( SqlTypes.VECTOR, "FLOAT32", "   ", mockDialect );
		assertNull( blankQuant.getQuantizationFormat() );
		assertEquals( "ARRAY<FLOAT32>(vector_length=>128)", blankQuant.getTypeName( size, null, null ) );
	}

	@Test
	@DisplayName("FLOAT64 non-quantized vectors continue to render cleanly and suppress quantization")
	public void testFloat64VectorsCleanRendering() {
		SpannerVectorDdlType float64Ddl = new SpannerVectorDdlType( SqlTypes.VECTOR_FLOAT64, "FLOAT64", mockDialect );

		Size size = new Size();
		size.setArrayLength( 128 );
		assertEquals( "ARRAY<FLOAT64>(vector_length=>128)", float64Ddl.getTypeName( size, null, null ) );

		assertEquals( "ARRAY<FLOAT64>", float64Ddl.getTypeName( new Size(), null, null ) );

		// Suppress quantization if precision set on FLOAT64
		size.setPrecision( 8 );
		assertEquals( "ARRAY<FLOAT64>(vector_length=>128)", float64Ddl.getTypeName( size, null, null ) );
	}

	// =========================================================================
	// 9. Cast Type Name Behavior
	// =========================================================================

	@Test
	@DisplayName("getCastTypeName never renders vector_length or quantization_format")
	public void testGetCastTypeNameWithAndWithoutQuantization() {
		Size size = new Size();
		size.setArrayLength( 128 );

		// Standard unquantized
		SpannerVectorDdlType stdDdl = new SpannerVectorDdlType( SqlTypes.VECTOR, "FLOAT32", mockDialect );
		assertEquals( "ARRAY<FLOAT32>", stdDdl.getCastTypeName( size, null, null ) );

		// SFP8 quantized
		SpannerVectorDdlType sfp8Ddl = new SpannerVectorDdlType( SqlTypes.VECTOR, "FLOAT32", "SFP8", mockDialect );
		assertEquals( "ARRAY<FLOAT32>", sfp8Ddl.getCastTypeName( size, null, null ) );

		// SFP4 quantized
		SpannerVectorDdlType sfp4Ddl = new SpannerVectorDdlType( SqlTypes.VECTOR, "FLOAT32", "SFP4", mockDialect );
		assertEquals( "ARRAY<FLOAT32>", sfp4Ddl.getCastTypeName( size, null, null ) );

		// FLOAT64
		SpannerVectorDdlType float64Ddl = new SpannerVectorDdlType( SqlTypes.VECTOR_FLOAT64, "FLOAT64", mockDialect );
		assertEquals( "ARRAY<FLOAT64>", float64Ddl.getCastTypeName( size, null, null ) );
	}

	// =========================================================================
	// 10. Validation and Adversarial Guardrails
	// =========================================================================

	@Test
	@DisplayName("Quantization format on FLOAT64 is rejected with IllegalArgumentException")
	public void testQuantizationOnFloat64Rejected() {
		IllegalArgumentException ex = assertThrows(
				IllegalArgumentException.class,
				() -> new SpannerVectorDdlType( SqlTypes.VECTOR_FLOAT64, "FLOAT64", "SFP8", mockDialect )
		);
		assertEquals( "Quantization format is only supported on ARRAY<FLOAT32>, not ARRAY<FLOAT64>", ex.getMessage() );

		// Programmatic invocation on FLOAT64 also rejected
		SpannerVectorDdlType float64Ddl = new SpannerVectorDdlType( SqlTypes.VECTOR_FLOAT64, "FLOAT64", mockDialect );
		Size size = new Size();
		size.setArrayLength( 128 );
		assertThrows(
				IllegalArgumentException.class,
				() -> float64Ddl.getTypeName( size, "SFP8" )
		);
	}

	@Test
	@DisplayName("Unsupported or invalid quantization formats are rejected")
	public void testInvalidQuantizationFormatsRejected() {
		// INT8 is unsupported in Spanner vector quantization
		assertThrows(
				IllegalArgumentException.class,
				() -> new SpannerVectorDdlType( SqlTypes.VECTOR, "FLOAT32", "INT8", mockDialect )
		);

		// Arbitrary / unknown strings
		assertThrows(
				IllegalArgumentException.class,
				() -> new SpannerVectorDdlType( SqlTypes.VECTOR, "FLOAT32", "UNKNOWN_FORMAT", mockDialect )
		);

		// SQL injection attempt
		assertThrows(
				IllegalArgumentException.class,
				() -> new SpannerVectorDdlType( SqlTypes.VECTOR, "FLOAT32", "SFP8') DROP TABLE users; --", mockDialect )
		);

		// Programmatic invalid format
		SpannerVectorDdlType ddlType = new SpannerVectorDdlType( SqlTypes.VECTOR, "FLOAT32", mockDialect );
		Size size = new Size();
		size.setArrayLength( 128 );
		assertThrows(
				IllegalArgumentException.class,
				() -> ddlType.getTypeName( size, "INVALID" )
		);
	}
}
