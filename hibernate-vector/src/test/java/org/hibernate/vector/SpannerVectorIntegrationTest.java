/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.vector;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Tuple;
import org.hibernate.annotations.Array;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.SpannerDialect;
import org.hibernate.dialect.SpannerPostgreSQLDialect;
import org.hibernate.sql.spi.SqlAppender;
import org.hibernate.sql.spi.StringBuilderSqlAppender;
import org.hibernate.testing.orm.junit.DomainModel;
import org.hibernate.testing.orm.junit.RequiresDialect;
import org.hibernate.testing.orm.junit.SessionFactory;
import org.hibernate.testing.orm.junit.SessionFactoryScope;
import org.hibernate.type.SqlTypes;
import org.hibernate.type.descriptor.WrapperOptions;
import org.hibernate.type.descriptor.java.DoublePrimitiveArrayJavaType;
import org.hibernate.type.descriptor.java.FloatPrimitiveArrayJavaType;
import org.hibernate.type.descriptor.java.JavaType;
import org.hibernate.type.descriptor.jdbc.JdbcLiteralFormatter;
import org.hibernate.type.descriptor.jdbc.JdbcType;
import org.hibernate.vector.internal.VectorHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hibernate.vector.VectorTestHelper.cosineDistance;
import static org.hibernate.vector.VectorTestHelper.euclideanDistance;
import static org.hibernate.vector.VectorTestHelper.euclideanNorm;
import static org.hibernate.vector.VectorTestHelper.euclideanSquaredDistance;
import static org.hibernate.vector.VectorTestHelper.innerProduct;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

@DomainModel(annotatedClasses = {
		SpannerVectorIntegrationTest.DimensionedVectorEntity.class,
		SpannerVectorIntegrationTest.UnconstrainedVectorEntity.class,
		SpannerVectorIntegrationTest.BoxedVectorEntity.class
})
@SessionFactory
@RequiresDialect(SpannerDialect.class)
@RequiresDialect(SpannerPostgreSQLDialect.class)
public class SpannerVectorIntegrationTest {

	private static final float[] V1 = new float[] { 1.0f, 2.0f, 3.0f };
	private static final float[] V2 = new float[] { 4.0f, 5.0f, 6.0f };
	private static final double[] D1 = new double[] { 1.0d, 2.0d, 3.0d };
	private static final double[] D2 = new double[] { 4.0d, 5.0d, 6.0d };

	@BeforeEach
	public void prepareData(SessionFactoryScope scope) {
		scope.inTransaction( em -> {
			em.persist( new DimensionedVectorEntity( 1L, V1, V1, D1 ) );
			em.persist( new DimensionedVectorEntity( 2L, V2, V2, D2 ) );

			em.persist( new UnconstrainedVectorEntity( 1L, new float[] { 1.0f, 2.0f, 3.0f, 4.0f }, new double[] { 10.0d, 20.0d } ) );

			em.persist( new BoxedVectorEntity( 1L, new Float[] { 1.0f, 2.0f, 3.0f }, new Double[] { 1.0d, 2.0d, 3.0d } ) );
		} );
	}

	@AfterEach
	public void cleanup(SessionFactoryScope scope) {
		scope.inTransaction( em -> {
			em.createMutationQuery( "delete from DimensionedVectorEntity" ).executeUpdate();
			em.createMutationQuery( "delete from UnconstrainedVectorEntity" ).executeUpdate();
			em.createMutationQuery( "delete from BoxedVectorEntity" ).executeUpdate();
		} );
	}

	@Test
	public void testDimensionedVectorsReadWrite(SessionFactoryScope scope) {
		scope.inTransaction( em -> {
			DimensionedVectorEntity entity = em.find( DimensionedVectorEntity.class, 1L );
			assertNotNull( entity );
			assertArrayEquals( V1, entity.getVFloat() );
			assertArrayEquals( V1, entity.getVFloat32() );
			assertArrayEquals( D1, entity.getVFloat64() );

			DimensionedVectorEntity entity2 = em.find( DimensionedVectorEntity.class, 2L );
			assertNotNull( entity2 );
			assertArrayEquals( V2, entity2.getVFloat() );
			assertArrayEquals( V2, entity2.getVFloat32() );
			assertArrayEquals( D2, entity2.getVFloat64() );
		} );
	}

	@Test
	public void testUnconstrainedVectorsReadWrite(SessionFactoryScope scope) {
		scope.inTransaction( em -> {
			UnconstrainedVectorEntity entity = em.find( UnconstrainedVectorEntity.class, 1L );
			assertNotNull( entity );
			assertArrayEquals( new float[] { 1.0f, 2.0f, 3.0f, 4.0f }, entity.getVUnconstrained() );
			assertArrayEquals( new double[] { 10.0d, 20.0d }, entity.getVUnconstrainedDouble() );
		} );
	}

	@Test
	public void testBoxedVectorsReadWrite(SessionFactoryScope scope) {
		scope.inTransaction( em -> {
			BoxedVectorEntity entity = em.find( BoxedVectorEntity.class, 1L );
			assertNotNull( entity );
			assertArrayEquals( new Float[] { 1.0f, 2.0f, 3.0f }, entity.getVBoxedFloat() );
			assertArrayEquals( new Double[] { 1.0d, 2.0d, 3.0d }, entity.getVBoxedDouble() );
		} );
	}

	@Test
	public void testNullVectorsReadWrite(SessionFactoryScope scope) {
		scope.inTransaction( em -> {
			em.persist( new DimensionedVectorEntity( 99L, null, null, null ) );
		} );
		scope.inTransaction( em -> {
			DimensionedVectorEntity entity = em.find( DimensionedVectorEntity.class, 99L );
			assertNotNull( entity );
			assertNull( entity.getVFloat() );
			assertNull( entity.getVFloat32() );
			assertNull( entity.getVFloat64() );
		} );
	}

	@Test
	public void testDistanceAndNormFunctions(SessionFactoryScope scope) {
		scope.inTransaction( em -> {
			final float[] queryVec = new float[] { 1.0f, 1.0f, 1.0f };
			final List<Tuple> results = em.createSelectionQuery(
							"select e.id, " +
									"cosine_distance(e.vFloat, :vec), " +
									"euclidean_distance(e.vFloat, :vec), " +
									"euclidean_squared_distance(e.vFloat, :vec), " +
									"inner_product(e.vFloat, :vec), " +
									"negative_inner_product(e.vFloat, :vec), " +
									"vector_dims(e.vFloat), " +
									"vector_norm(e.vFloat), " +
									"l2_norm(e.vFloat) " +
									"from DimensionedVectorEntity e order by e.id",
							Tuple.class
					)
					.setParameter( "vec", queryVec )
					.getResultList();

			assertEquals( 2, results.size() );

			Tuple row1 = results.get( 0 );
			assertEquals( 1L, row1.get( 0 ) );
			assertEquals( cosineDistance( V1, queryVec ), row1.get( 1, double.class ), 0.00001D );
			assertEquals( euclideanDistance( V1, queryVec ), row1.get( 2, double.class ), 0.00001D );
			assertEquals( euclideanSquaredDistance( V1, queryVec ), row1.get( 3, double.class ), 0.00001D );
			assertEquals( innerProduct( V1, queryVec ), row1.get( 4, double.class ), 0.00001D );
			assertEquals( innerProduct( V1, queryVec ) * -1, row1.get( 5, double.class ), 0.00001D );
			assertEquals( 3, row1.get( 6 ) );
			assertEquals( euclideanNorm( V1 ), row1.get( 7, double.class ), 0.00001D );
			assertEquals( euclideanNorm( V1 ), row1.get( 8, double.class ), 0.00001D );
		} );
	}

	@Test
	public void testCastPatterns(SessionFactoryScope scope) {
		scope.inTransaction( em -> {
			Tuple tuple = em.createSelectionQuery(
							"select cast(e.vFloat as string), " +
									"cast('[1, 1, 1]' as vector(3)), " +
									"cast('[2, 2, 2]' as float_vector(3)), " +
									"cast('[3, 3, 3]' as double_vector(3)) " +
									"from DimensionedVectorEntity e where e.id = 1",
							Tuple.class
					)
					.getSingleResult();

			assertNotNull( tuple.get( 0, String.class ) );
			assertArrayEquals( new float[] { 1.0f, 2.0f, 3.0f }, VectorHelper.parseFloatVector( tuple.get( 0, String.class ) ) );
			assertArrayEquals( new float[] { 1.0f, 1.0f, 1.0f }, tuple.get( 1, float[].class ) );
			assertArrayEquals( new float[] { 2.0f, 2.0f, 2.0f }, tuple.get( 2, float[].class ) );
			assertArrayEquals( new double[] { 3.0d, 3.0d, 3.0d }, tuple.get( 3, double[].class ) );
		} );
	}

	@Test
	public void testDoubleVectorDistances(SessionFactoryScope scope) {
		scope.inTransaction( em -> {
			final double[] queryVec = new double[] { 1.0d, 1.0d, 1.0d };
			final List<Tuple> results = em.createSelectionQuery(
							"select e.id, " +
									"cosine_distance(e.vFloat64, :vec), " +
									"euclidean_distance(e.vFloat64, :vec), " +
									"inner_product(e.vFloat64, :vec), " +
									"vector_dims(e.vFloat64), " +
									"vector_norm(e.vFloat64) " +
									"from DimensionedVectorEntity e order by e.id",
							Tuple.class
					)
					.setParameter( "vec", queryVec )
					.getResultList();

			assertEquals( 2, results.size() );
			Tuple row1 = results.get( 0 );
			assertEquals( 1L, row1.get( 0 ) );
			assertEquals( cosineDistance( new float[] { 1.0f, 2.0f, 3.0f }, new float[] { 1.0f, 1.0f, 1.0f } ), row1.get( 1, double.class ), 0.00001D );
			assertEquals( euclideanDistance( new float[] { 1.0f, 2.0f, 3.0f }, new float[] { 1.0f, 1.0f, 1.0f } ), row1.get( 2, double.class ), 0.00001D );
			assertEquals( innerProduct( new float[] { 1.0f, 2.0f, 3.0f }, new float[] { 1.0f, 1.0f, 1.0f } ), row1.get( 3, double.class ), 0.00001D );
			assertEquals( 3, row1.get( 4 ) );
			assertEquals( euclideanNorm( new float[] { 1.0f, 2.0f, 3.0f } ), row1.get( 5, double.class ), 0.00001D );
		} );
	}

	@Test
	public void testLiteralFormattingAgainstDatabase(SessionFactoryScope scope) {
		scope.inTransaction( em -> {
			Dialect dialect = scope.getSessionFactory().getJdbcServices().getDialect();
			WrapperOptions wrapperOptions = scope.getSessionFactory().getWrapperOptions();

			// 1. Float primitive array literal formatting
			JdbcType floatJdbcType = scope.getSessionFactory().getTypeConfiguration().getJdbcTypeRegistry().getDescriptor( SqlTypes.VECTOR );
			JdbcLiteralFormatter<float[]> floatFormatter = floatJdbcType.getJdbcLiteralFormatter( FloatPrimitiveArrayJavaType.INSTANCE );
			SqlAppender floatAppender = new StringBuilderSqlAppender();
			floatFormatter.appendJdbcLiteral( floatAppender, new float[] { 1.0f, 2.0f, 3.0f }, dialect, wrapperOptions );
			String floatLiteralSql = floatAppender.toString();
			Object floatResult = em.createNativeQuery( "SELECT " + floatLiteralSql ).getSingleResult();
			assertNotNull( floatResult );

			// 2. Double primitive array literal formatting
			JdbcType doubleJdbcType = scope.getSessionFactory().getTypeConfiguration().getJdbcTypeRegistry().getDescriptor( SqlTypes.VECTOR_FLOAT64 );
			JdbcLiteralFormatter<double[]> doubleFormatter = doubleJdbcType.getJdbcLiteralFormatter( DoublePrimitiveArrayJavaType.INSTANCE );
			SqlAppender doubleAppender = new StringBuilderSqlAppender();
			doubleFormatter.appendJdbcLiteral( doubleAppender, new double[] { 1.0d, 2.0d, 3.0d }, dialect, wrapperOptions );
			String doubleLiteralSql = doubleAppender.toString();
			Object doubleResult = em.createNativeQuery( "SELECT " + doubleLiteralSql ).getSingleResult();
			assertNotNull( doubleResult );

			// 3. Boxed Float array literal formatting
			JavaType<Float[]> boxedFloatJavaType = scope.getSessionFactory().getTypeConfiguration().getJavaTypeRegistry().resolveDescriptor( Float[].class );
			JdbcLiteralFormatter<Float[]> boxedFloatFormatter = floatJdbcType.getJdbcLiteralFormatter( boxedFloatJavaType );
			SqlAppender boxedFloatAppender = new StringBuilderSqlAppender();
			boxedFloatFormatter.appendJdbcLiteral( boxedFloatAppender, new Float[] { 1.0f, 2.0f, 3.0f }, dialect, wrapperOptions );
			String boxedFloatLiteralSql = boxedFloatAppender.toString();
			Object boxedFloatResult = em.createNativeQuery( "SELECT " + boxedFloatLiteralSql ).getSingleResult();
			assertNotNull( boxedFloatResult );
		} );
	}

	@Entity(name = "DimensionedVectorEntity")
	public static class DimensionedVectorEntity {
		@Id
		private Long id;

		@Column(name = "v_float")
		@JdbcTypeCode(SqlTypes.VECTOR)
		@Array(length = 3)
		private float[] vFloat;

		@Column(name = "v_float32")
		@JdbcTypeCode(SqlTypes.VECTOR_FLOAT32)
		@Array(length = 3)
		private float[] vFloat32;

		@Column(name = "v_float64")
		@JdbcTypeCode(SqlTypes.VECTOR_FLOAT64)
		@Array(length = 3)
		private double[] vFloat64;

		public DimensionedVectorEntity() {}

		public DimensionedVectorEntity(Long id, float[] vFloat, float[] vFloat32, double[] vFloat64) {
			this.id = id;
			this.vFloat = vFloat;
			this.vFloat32 = vFloat32;
			this.vFloat64 = vFloat64;
		}

		public Long getId() { return id; }
		public float[] getVFloat() { return vFloat; }
		public float[] getVFloat32() { return vFloat32; }
		public double[] getVFloat64() { return vFloat64; }
	}

	@Entity(name = "UnconstrainedVectorEntity")
	public static class UnconstrainedVectorEntity {
		@Id
		private Long id;

		@Column(name = "v_unconstrained")
		@JdbcTypeCode(SqlTypes.VECTOR)
		private float[] vUnconstrained;

		@Column(name = "v_unconstrained_double")
		@JdbcTypeCode(SqlTypes.VECTOR_FLOAT64)
		private double[] vUnconstrainedDouble;

		public UnconstrainedVectorEntity() {}

		public UnconstrainedVectorEntity(Long id, float[] vUnconstrained, double[] vUnconstrainedDouble) {
			this.id = id;
			this.vUnconstrained = vUnconstrained;
			this.vUnconstrainedDouble = vUnconstrainedDouble;
		}

		public Long getId() { return id; }
		public float[] getVUnconstrained() { return vUnconstrained; }
		public double[] getVUnconstrainedDouble() { return vUnconstrainedDouble; }
	}

	@Entity(name = "BoxedVectorEntity")
	public static class BoxedVectorEntity {
		@Id
		private Long id;

		@Column(name = "v_boxed_float")
		@JdbcTypeCode(SqlTypes.VECTOR)
		@Array(length = 3)
		private Float[] vBoxedFloat;

		@Column(name = "v_boxed_double")
		@JdbcTypeCode(SqlTypes.VECTOR_FLOAT64)
		@Array(length = 3)
		private Double[] vBoxedDouble;

		public BoxedVectorEntity() {}

		public BoxedVectorEntity(Long id, Float[] vBoxedFloat, Double[] vBoxedDouble) {
			this.id = id;
			this.vBoxedFloat = vBoxedFloat;
			this.vBoxedDouble = vBoxedDouble;
		}

		public Long getId() { return id; }
		public Float[] getVBoxedFloat() { return vBoxedFloat; }
		public Double[] getVBoxedDouble() { return vBoxedDouble; }
	}
}
