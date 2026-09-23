/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.vector.internal;

import org.hibernate.boot.model.TypeContributions;
import org.hibernate.dialect.PostgreSQLDialect;
import org.hibernate.dialect.SpannerDialect;
import org.hibernate.dialect.SpannerPostgreSQLDialect;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.type.BasicArrayType;
import org.hibernate.type.BasicType;
import org.hibernate.type.SqlTypes;
import org.hibernate.type.descriptor.jdbc.JdbcType;
import org.hibernate.type.descriptor.sql.DdlType;
import org.hibernate.type.spi.TypeConfiguration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SpannerPostgreSQLVectorTypeContributorTest {

	@Test
	public void testContributeForSpannerPostgreSQLDialect() {
		SpannerPostgreSQLDialect mockDialect = mock( SpannerPostgreSQLDialect.class );
		JdbcServices jdbcServices = mock( JdbcServices.class );
		when( jdbcServices.getDialect() ).thenReturn( mockDialect );

		ServiceRegistry serviceRegistry = mock( ServiceRegistry.class );
		when( serviceRegistry.requireService( JdbcServices.class ) ).thenReturn( jdbcServices );

		TypeConfiguration typeConfiguration = new TypeConfiguration();
		TypeContributions typeContributions = mock( TypeContributions.class );
		when( typeContributions.getTypeConfiguration() ).thenReturn( typeConfiguration );

		SpannerPostgreSQLVectorTypeContributor contributor = new SpannerPostgreSQLVectorTypeContributor();
		contributor.contribute( typeContributions, serviceRegistry );

		// 1. Verify JdbcType registrations
		JdbcType vectorJdbcType = typeConfiguration.getJdbcTypeRegistry().getDescriptor( SqlTypes.VECTOR );
		assertNotNull( vectorJdbcType );
		assertTrue( vectorJdbcType instanceof SpannerPostgreSQLVectorJdbcType );
		assertEquals( "float4", ((SpannerPostgreSQLVectorJdbcType) vectorJdbcType).getElementTypeName( null, null ) );

		JdbcType float32JdbcType = typeConfiguration.getJdbcTypeRegistry().getDescriptor( SqlTypes.VECTOR_FLOAT32 );
		assertNotNull( float32JdbcType );
		assertTrue( float32JdbcType instanceof SpannerPostgreSQLVectorJdbcType );
		assertEquals( "float4", ((SpannerPostgreSQLVectorJdbcType) float32JdbcType).getElementTypeName( null, null ) );

		JdbcType float64JdbcType = typeConfiguration.getJdbcTypeRegistry().getDescriptor( SqlTypes.VECTOR_FLOAT64 );
		assertNotNull( float64JdbcType );
		assertTrue( float64JdbcType instanceof SpannerPostgreSQLVectorJdbcType );
		assertEquals( "float8", ((SpannerPostgreSQLVectorJdbcType) float64JdbcType).getElementTypeName( null, null ) );

		// 2. Verify BasicType registrations
		BasicType<?> vectorBasicType = typeConfiguration.getBasicTypeRegistry().getRegisteredType( "vector" );
		assertNotNull( vectorBasicType );
		assertTrue( vectorBasicType instanceof BasicArrayType );

		BasicType<?> float32BasicType = typeConfiguration.getBasicTypeRegistry().getRegisteredType( "float_vector" );
		assertNotNull( float32BasicType );
		assertTrue( float32BasicType instanceof BasicArrayType );

		BasicType<?> float64BasicType = typeConfiguration.getBasicTypeRegistry().getRegisteredType( "double_vector" );
		assertNotNull( float64BasicType );
		assertTrue( float64BasicType instanceof BasicArrayType );

		// 3. Verify DdlType registrations
		DdlType vectorDdlType = typeConfiguration.getDdlTypeRegistry().getDescriptor( SqlTypes.VECTOR );
		assertNotNull( vectorDdlType );
		assertTrue( vectorDdlType instanceof SpannerPostgreSQLVectorDdlType );

		DdlType float32DdlType = typeConfiguration.getDdlTypeRegistry().getDescriptor( SqlTypes.VECTOR_FLOAT32 );
		assertNotNull( float32DdlType );
		assertTrue( float32DdlType instanceof SpannerPostgreSQLVectorDdlType );

		DdlType float64DdlType = typeConfiguration.getDdlTypeRegistry().getDescriptor( SqlTypes.VECTOR_FLOAT64 );
		assertNotNull( float64DdlType );
		assertTrue( float64DdlType instanceof SpannerPostgreSQLVectorDdlType );
	}

	@Test
	public void testDoNotContributeForStandardPostgreSQLDialect() {
		PostgreSQLDialect mockDialect = mock( PostgreSQLDialect.class );
		JdbcServices jdbcServices = mock( JdbcServices.class );
		when( jdbcServices.getDialect() ).thenReturn( mockDialect );

		ServiceRegistry serviceRegistry = mock( ServiceRegistry.class );
		when( serviceRegistry.requireService( JdbcServices.class ) ).thenReturn( jdbcServices );

		TypeConfiguration typeConfiguration = new TypeConfiguration();
		TypeContributions typeContributions = mock( TypeContributions.class );
		when( typeContributions.getTypeConfiguration() ).thenReturn( typeConfiguration );

		SpannerPostgreSQLVectorTypeContributor contributor = new SpannerPostgreSQLVectorTypeContributor();
		contributor.contribute( typeContributions, serviceRegistry );

		assertNull( typeConfiguration.getDdlTypeRegistry().getDescriptor( SqlTypes.VECTOR ) );
		assertNull( typeConfiguration.getDdlTypeRegistry().getDescriptor( SqlTypes.VECTOR_FLOAT32 ) );
		assertNull( typeConfiguration.getDdlTypeRegistry().getDescriptor( SqlTypes.VECTOR_FLOAT64 ) );
		assertFalse( typeConfiguration.getJdbcTypeRegistry().getDescriptor( SqlTypes.VECTOR ) instanceof SpannerPostgreSQLVectorJdbcType );
		assertFalse( typeConfiguration.getJdbcTypeRegistry().getDescriptor( SqlTypes.VECTOR_FLOAT32 ) instanceof SpannerPostgreSQLVectorJdbcType );
		assertFalse( typeConfiguration.getJdbcTypeRegistry().getDescriptor( SqlTypes.VECTOR_FLOAT64 ) instanceof SpannerPostgreSQLVectorJdbcType );
		assertFalse( typeConfiguration.getBasicTypeRegistry().getRegisteredType( "vector" ).getJdbcType() instanceof SpannerPostgreSQLVectorJdbcType );
		assertFalse( typeConfiguration.getBasicTypeRegistry().getRegisteredType( "float_vector" ).getJdbcType() instanceof SpannerPostgreSQLVectorJdbcType );
		assertFalse( typeConfiguration.getBasicTypeRegistry().getRegisteredType( "double_vector" ).getJdbcType() instanceof SpannerPostgreSQLVectorJdbcType );
	}

	@Test
	public void testDoNotContributeForSpannerGoogleSQLDialect() {
		SpannerDialect mockDialect = mock( SpannerDialect.class );
		JdbcServices jdbcServices = mock( JdbcServices.class );
		when( jdbcServices.getDialect() ).thenReturn( mockDialect );

		ServiceRegistry serviceRegistry = mock( ServiceRegistry.class );
		when( serviceRegistry.requireService( JdbcServices.class ) ).thenReturn( jdbcServices );

		TypeConfiguration typeConfiguration = new TypeConfiguration();
		TypeContributions typeContributions = mock( TypeContributions.class );
		when( typeContributions.getTypeConfiguration() ).thenReturn( typeConfiguration );

		SpannerPostgreSQLVectorTypeContributor contributor = new SpannerPostgreSQLVectorTypeContributor();
		contributor.contribute( typeContributions, serviceRegistry );

		assertNull( typeConfiguration.getDdlTypeRegistry().getDescriptor( SqlTypes.VECTOR ) );
		assertNull( typeConfiguration.getDdlTypeRegistry().getDescriptor( SqlTypes.VECTOR_FLOAT32 ) );
		assertNull( typeConfiguration.getDdlTypeRegistry().getDescriptor( SqlTypes.VECTOR_FLOAT64 ) );
		assertFalse( typeConfiguration.getJdbcTypeRegistry().getDescriptor( SqlTypes.VECTOR ) instanceof SpannerPostgreSQLVectorJdbcType );
		assertFalse( typeConfiguration.getJdbcTypeRegistry().getDescriptor( SqlTypes.VECTOR_FLOAT32 ) instanceof SpannerPostgreSQLVectorJdbcType );
		assertFalse( typeConfiguration.getJdbcTypeRegistry().getDescriptor( SqlTypes.VECTOR_FLOAT64 ) instanceof SpannerPostgreSQLVectorJdbcType );
		assertFalse( typeConfiguration.getBasicTypeRegistry().getRegisteredType( "vector" ).getJdbcType() instanceof SpannerPostgreSQLVectorJdbcType );
		assertFalse( typeConfiguration.getBasicTypeRegistry().getRegisteredType( "float_vector" ).getJdbcType() instanceof SpannerPostgreSQLVectorJdbcType );
		assertFalse( typeConfiguration.getBasicTypeRegistry().getRegisteredType( "double_vector" ).getJdbcType() instanceof SpannerPostgreSQLVectorJdbcType );
	}

	@Test
	public void testDoNotContributeForNullOrUnrelatedDialect() {
		JdbcServices jdbcServices = mock( JdbcServices.class );
		when( jdbcServices.getDialect() ).thenReturn( null );

		ServiceRegistry serviceRegistry = mock( ServiceRegistry.class );
		when( serviceRegistry.requireService( JdbcServices.class ) ).thenReturn( jdbcServices );

		TypeConfiguration typeConfiguration = new TypeConfiguration();
		TypeContributions typeContributions = mock( TypeContributions.class );
		when( typeContributions.getTypeConfiguration() ).thenReturn( typeConfiguration );

		SpannerPostgreSQLVectorTypeContributor contributor = new SpannerPostgreSQLVectorTypeContributor();

		// Case 1: Null dialect must not crash and must not register
		contributor.contribute( typeContributions, serviceRegistry );
		assertNull( typeConfiguration.getDdlTypeRegistry().getDescriptor( SqlTypes.VECTOR ) );

		// Case 2: Unrelated Dialect (e.g. H2Dialect) must not register
		when( jdbcServices.getDialect() ).thenReturn( mock( org.hibernate.dialect.H2Dialect.class ) );
		contributor.contribute( typeContributions, serviceRegistry );
		assertNull( typeConfiguration.getDdlTypeRegistry().getDescriptor( SqlTypes.VECTOR ) );
	}
}
