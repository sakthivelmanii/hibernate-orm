/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.vector.internal;

import java.util.List;

import org.hibernate.boot.model.FunctionContributions;
import org.hibernate.dialect.PostgreSQLDialect;
import org.hibernate.dialect.SpannerDialect;
import org.hibernate.dialect.SpannerPostgreSQLDialect;
import org.hibernate.query.sqm.function.AbstractSqmFunctionDescriptor;
import org.hibernate.query.sqm.function.SqmFunctionRegistry;
import org.hibernate.query.sqm.produce.function.PatternFunctionDescriptorBuilder;
import org.hibernate.type.BasicType;
import org.hibernate.type.BasicTypeRegistry;
import org.hibernate.type.StandardBasicTypes;
import org.hibernate.type.spi.TypeConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SpannerVectorFunctionContributorTest {

	private FunctionContributions functionContributions;
	private SqmFunctionRegistry functionRegistry;
	private TypeConfiguration typeConfiguration;
	private BasicTypeRegistry basicTypeRegistry;
	private PatternFunctionDescriptorBuilder patternBuilder;

	@BeforeEach
	@SuppressWarnings("unchecked")
	public void setup() {
		functionContributions = mock( FunctionContributions.class );
		functionRegistry = mock( SqmFunctionRegistry.class );
		typeConfiguration = mock( TypeConfiguration.class );
		basicTypeRegistry = mock( BasicTypeRegistry.class );
		patternBuilder = mock( PatternFunctionDescriptorBuilder.class );

		when( functionContributions.getFunctionRegistry() ).thenReturn( functionRegistry );
		when( functionContributions.getTypeConfiguration() ).thenReturn( typeConfiguration );
		when( typeConfiguration.getBasicTypeRegistry() ).thenReturn( basicTypeRegistry );

		BasicType<Integer> integerType = mock( BasicType.class );
		BasicType<Double> doubleType = mock( BasicType.class );
		when( basicTypeRegistry.resolve( StandardBasicTypes.INTEGER ) ).thenReturn( integerType );
		when( basicTypeRegistry.resolve( StandardBasicTypes.DOUBLE ) ).thenReturn( doubleType );

		when( functionRegistry.patternDescriptorBuilder( anyString(), anyString() ) ).thenReturn( patternBuilder );
		when( patternBuilder.setArgumentsValidator( any() ) ).thenReturn( patternBuilder );
		when( patternBuilder.setArgumentTypeResolver( any() ) ).thenReturn( patternBuilder );
		when( patternBuilder.setReturnTypeResolver( any() ) ).thenReturn( patternBuilder );
	}

	@Test
	public void testContributeFunctionsForSpannerDialect() {
		SpannerDialect dialect = mock( SpannerDialect.class );
		when( functionContributions.getDialect() ).thenReturn( dialect );

		SpannerVectorFunctionContributor contributor = new SpannerVectorFunctionContributor();
		contributor.contributeFunctions( functionContributions );

		// Exact distance functions
		verify( functionRegistry ).patternDescriptorBuilder( eq( "cosine_distance" ), eq( "COSINE_DISTANCE(?1, ?2)" ) );
		verify( functionRegistry ).patternDescriptorBuilder( eq( "euclidean_distance" ), eq( "EUCLIDEAN_DISTANCE(?1, ?2)" ) );
		verify( functionRegistry ).patternDescriptorBuilder( eq( "euclidean_squared_distance" ), eq( "POW(EUCLIDEAN_DISTANCE(?1, ?2), 2)" ) );
		verify( functionRegistry ).patternDescriptorBuilder( eq( "inner_product" ), eq( "DOT_PRODUCT(?1, ?2)" ) );
		verify( functionRegistry ).patternDescriptorBuilder( eq( "negative_inner_product" ), eq( "(DOT_PRODUCT(?1, ?2) * -1)" ) );

		// Alternate keys
		verify( functionRegistry ).registerAlternateKey( "l2_distance", "euclidean_distance" );
		verify( functionRegistry ).registerAlternateKey( "l2_squared_distance", "euclidean_squared_distance" );
		verify( functionRegistry ).registerAlternateKey( "l2_norm", "vector_norm" );

		// ANN distance functions registered as SpannerGoogleSqlApproxDistanceFunction
		verify( functionRegistry ).register( eq( "approx_cosine_distance" ), any( SpannerGoogleSqlApproxDistanceFunction.class ) );
		verify( functionRegistry ).register( eq( "approx_euclidean_distance" ), any( SpannerGoogleSqlApproxDistanceFunction.class ) );
		verify( functionRegistry ).register( eq( "approx_dot_product" ), any( SpannerGoogleSqlApproxDistanceFunction.class ) );

		// Dimension and Norm functions
		verify( functionRegistry ).patternDescriptorBuilder( eq( "vector_dims" ), eq( "ARRAY_LENGTH(?1)" ) );
		verify( functionRegistry ).patternDescriptorBuilder( eq( "vector_norm" ), eq( "SQRT(DOT_PRODUCT(?1, ?1))" ) );
	}

	@Test
	public void testDoesNotContributeForSpannerPostgreSQLDialect() {
		SpannerPostgreSQLDialect dialect = mock( SpannerPostgreSQLDialect.class );
		when( functionContributions.getDialect() ).thenReturn( dialect );

		SpannerVectorFunctionContributor contributor = new SpannerVectorFunctionContributor();
		contributor.contributeFunctions( functionContributions );

		verify( functionRegistry, never() ).patternDescriptorBuilder( anyString(), anyString() );
		verify( functionRegistry, never() ).register( anyString(), any( org.hibernate.query.sqm.function.SqmFunctionDescriptor.class ) );
	}

	@Test
	public void testDoesNotContributeForStandardPostgreSQLDialect() {
		PostgreSQLDialect dialect = mock( PostgreSQLDialect.class );
		when( functionContributions.getDialect() ).thenReturn( dialect );

		SpannerVectorFunctionContributor contributor = new SpannerVectorFunctionContributor();
		contributor.contributeFunctions( functionContributions );

		verify( functionRegistry, never() ).patternDescriptorBuilder( anyString(), anyString() );
		verify( functionRegistry, never() ).register( anyString(), any( org.hibernate.query.sqm.function.SqmFunctionDescriptor.class ) );
	}

	@Test
	public void testOrdinal() {
		SpannerVectorFunctionContributor contributor = new SpannerVectorFunctionContributor();
		assertEquals( 200, contributor.ordinal() );
	}

	@Test
	public void testDoesNotContributeForNullDialect() {
		when( functionContributions.getDialect() ).thenReturn( null );

		SpannerVectorFunctionContributor contributor = new SpannerVectorFunctionContributor();
		contributor.contributeFunctions( functionContributions );

		verify( functionRegistry, never() ).patternDescriptorBuilder( anyString(), anyString() );
		verify( functionRegistry, never() ).register( anyString(), any( org.hibernate.query.sqm.function.SqmFunctionDescriptor.class ) );
	}

	@Test
	public void testFunctionReturnTypesWithRealRegistry() {
		TypeConfiguration realTypeConfig = new TypeConfiguration();
		SqmFunctionRegistry realRegistry = new SqmFunctionRegistry();
		FunctionContributions realContribs = mock( FunctionContributions.class );
		when( realContribs.getFunctionRegistry() ).thenReturn( realRegistry );
		when( realContribs.getTypeConfiguration() ).thenReturn( realTypeConfig );
		when( realContribs.getDialect() ).thenReturn( mock( SpannerDialect.class ) );

		SpannerVectorFunctionContributor contributor = new SpannerVectorFunctionContributor();
		contributor.contributeFunctions( realContribs );

		final BasicType<Integer> expectedIntegerType = realTypeConfig.getBasicTypeRegistry().resolve( StandardBasicTypes.INTEGER );
		final BasicType<Double> expectedDoubleType = realTypeConfig.getBasicTypeRegistry().resolve( StandardBasicTypes.DOUBLE );

		// 1. vector_dims returns Integer
		AbstractSqmFunctionDescriptor dims = (AbstractSqmFunctionDescriptor) realRegistry.findFunctionDescriptor( "vector_dims" );
		assertNotNull( dims );
		BasicType<?> dimsReturnType = (BasicType<?>) dims.getReturnTypeResolver().resolveFunctionReturnType( null, (org.hibernate.query.sqm.sql.spi.SqmToSqlAstConverter) null, null, realTypeConfig );
		assertEquals( expectedIntegerType, dimsReturnType );
		assertEquals( Integer.class, dimsReturnType.getJavaType() );

		// 2. vector_norm returns Double
		AbstractSqmFunctionDescriptor norm = (AbstractSqmFunctionDescriptor) realRegistry.findFunctionDescriptor( "vector_norm" );
		assertNotNull( norm );
		BasicType<?> normReturnType = (BasicType<?>) norm.getReturnTypeResolver().resolveFunctionReturnType( null, (org.hibernate.query.sqm.sql.spi.SqmToSqlAstConverter) null, null, realTypeConfig );
		assertEquals( expectedDoubleType, normReturnType );
		assertEquals( Double.class, normReturnType.getJavaType() );

		// 3. l2_norm alias resolves to vector_norm and returns Double
		AbstractSqmFunctionDescriptor l2Norm = (AbstractSqmFunctionDescriptor) realRegistry.findFunctionDescriptor( "l2_norm" );
		assertNotNull( l2Norm );
		BasicType<?> l2NormReturnType = (BasicType<?>) l2Norm.getReturnTypeResolver().resolveFunctionReturnType( null, (org.hibernate.query.sqm.sql.spi.SqmToSqlAstConverter) null, null, realTypeConfig );
		assertEquals( expectedDoubleType, l2NormReturnType );
		assertEquals( Double.class, l2NormReturnType.getJavaType() );

		// 4. ANN functions are SpannerGoogleSqlApproxDistanceFunction and return Double
		for ( String annFunc : List.of( "approx_cosine_distance", "approx_euclidean_distance", "approx_dot_product" ) ) {
			AbstractSqmFunctionDescriptor desc = (AbstractSqmFunctionDescriptor) realRegistry.findFunctionDescriptor( annFunc );
			assertNotNull( desc );
			assertInstanceOf( SpannerGoogleSqlApproxDistanceFunction.class, desc );
			BasicType<?> ret = (BasicType<?>) desc.getReturnTypeResolver().resolveFunctionReturnType( null, (org.hibernate.query.sqm.sql.spi.SqmToSqlAstConverter) null, null, realTypeConfig );
			assertEquals( expectedDoubleType, ret );
			assertEquals( Double.class, ret.getJavaType() );
		}

		// 5. Alternate keys resolution check
		assertSame( realRegistry.findFunctionDescriptor( "euclidean_distance" ), realRegistry.findFunctionDescriptor( "l2_distance" ) );
		assertSame( realRegistry.findFunctionDescriptor( "euclidean_squared_distance" ), realRegistry.findFunctionDescriptor( "l2_squared_distance" ) );
		assertSame( realRegistry.findFunctionDescriptor( "vector_norm" ), realRegistry.findFunctionDescriptor( "l2_norm" ) );
	}
}
