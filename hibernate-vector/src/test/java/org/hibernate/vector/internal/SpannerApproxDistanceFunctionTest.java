/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.vector.internal;

import java.util.List;

import org.hibernate.metamodel.mapping.MappingModelExpressible;
import org.hibernate.query.sqm.produce.function.FunctionArgumentTypeResolver;
import org.hibernate.query.sqm.sql.spi.SqmToSqlAstConverter;
import org.hibernate.query.sqm.tree.spi.SqmTypedNode;
import org.hibernate.sql.ast.spi.SqlAstNode;
import org.hibernate.sql.ast.spi.query.expression.Literal;
import org.hibernate.sql.ast.spi.translation.SqlAstTranslator;
import org.hibernate.sql.spi.SqlAppender;
import org.hibernate.sql.spi.StringBuilderSqlAppender;
import org.hibernate.type.BasicType;
import org.hibernate.type.StandardBasicTypes;
import org.hibernate.type.spi.TypeConfiguration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SpannerApproxDistanceFunctionTest {

	private TypeConfiguration typeConfiguration;
	private SqlAstTranslator<?> walker;

	@BeforeEach
	public void setUp() {
		typeConfiguration = new TypeConfiguration();
		walker = mock( SqlAstTranslator.class );
	}

	private SqlAstNode node(String sql, SqlAppender appender) {
		return w -> appender.appendSql( sql );
	}

	private Literal numericLiteral(Number value) {
		Literal literal = mock( Literal.class );
		when( literal.getLiteralValue() ).thenReturn( value );
		return literal;
	}

	private Literal stringLiteral(String sql, SqlAppender appender) {
		Literal literal = mock( Literal.class );
		when( literal.getLiteralValue() ).thenReturn( sql );
		doAnswer( invocation -> {
			appender.appendSql( sql );
			return null;
		} ).when( literal ).accept( any() );
		return literal;
	}

	private SqlAstNode expressionNode(String sql, SqlAppender appender) {
		SqlAstNode node = mock( SqlAstNode.class );
		doAnswer( invocation -> {
			appender.appendSql( sql );
			return null;
		} ).when( node ).accept( any() );
		return node;
	}

	// ------------------------------------------------------------------------
	// GoogleSQL Dialect Tests
	// ------------------------------------------------------------------------

	@Test
	@DisplayName("GoogleSQL: approx_cosine_distance with 2 arguments renders default options")
	public void testGoogleSqlApproxCosineDistance2Args() {
		SpannerGoogleSqlApproxDistanceFunction function = new SpannerGoogleSqlApproxDistanceFunction(
				"approx_cosine_distance",
				"APPROX_COSINE_DISTANCE",
				typeConfiguration
		);

		StringBuilderSqlAppender appender = new StringBuilderSqlAppender();
		List<SqlAstNode> args = List.of( node( "?1", appender ), node( "?2", appender ) );

		function.render( appender, args, null, walker );
		assertEquals(
				"APPROX_COSINE_DISTANCE(?1, ?2, OPTIONS => 'num_leaves_to_search: 100')",
				appender.toString()
		);
	}

	@Test
	@DisplayName("GoogleSQL: approx_cosine_distance with 3 arguments (integer literal) renders custom options")
	public void testGoogleSqlApproxCosineDistance3ArgsNumeric() {
		SpannerGoogleSqlApproxDistanceFunction function = new SpannerGoogleSqlApproxDistanceFunction(
				"approx_cosine_distance",
				"APPROX_COSINE_DISTANCE",
				typeConfiguration
		);

		StringBuilderSqlAppender appender = new StringBuilderSqlAppender();
		List<SqlAstNode> args = List.of( node( "?1", appender ), node( "?2", appender ), numericLiteral( 150 ) );

		function.render( appender, args, null, walker );
		assertEquals(
				"APPROX_COSINE_DISTANCE(?1, ?2, OPTIONS => 'num_leaves_to_search: 150')",
				appender.toString()
		);
	}

	@Test
	@DisplayName("GoogleSQL: approx_cosine_distance with 3 arguments (string literal) renders options verbatim")
	public void testGoogleSqlApproxCosineDistance3ArgsString() {
		SpannerGoogleSqlApproxDistanceFunction function = new SpannerGoogleSqlApproxDistanceFunction(
				"approx_cosine_distance",
				"APPROX_COSINE_DISTANCE",
				typeConfiguration
		);

		StringBuilderSqlAppender appender = new StringBuilderSqlAppender();
		List<SqlAstNode> args = List.of(
				node( "?1", appender ),
				node( "?2", appender ),
				stringLiteral( "'num_leaves_to_search: 250'", appender )
		);

		function.render( appender, args, null, walker );
		assertEquals(
				"APPROX_COSINE_DISTANCE(?1, ?2, OPTIONS => 'num_leaves_to_search: 250')",
				appender.toString()
		);
	}

	@Test
	@DisplayName("GoogleSQL: approx_euclidean_distance with 2 and 3 arguments")
	public void testGoogleSqlApproxEuclideanDistance() {
		SpannerGoogleSqlApproxDistanceFunction function = new SpannerGoogleSqlApproxDistanceFunction(
				"approx_euclidean_distance",
				"APPROX_EUCLIDEAN_DISTANCE",
				typeConfiguration
		);

		// 2 args
		StringBuilderSqlAppender appender2 = new StringBuilderSqlAppender();
		function.render( appender2, List.of( node( "?1", appender2 ), node( "?2", appender2 ) ), null, walker );
		assertEquals(
				"APPROX_EUCLIDEAN_DISTANCE(?1, ?2, OPTIONS => 'num_leaves_to_search: 100')",
				appender2.toString()
		);

		// 3 args numeric
		StringBuilderSqlAppender appender3 = new StringBuilderSqlAppender();
		function.render( appender3, List.of( node( "?1", appender3 ), node( "?2", appender3 ), numericLiteral( 200 ) ), null, walker );
		assertEquals(
				"APPROX_EUCLIDEAN_DISTANCE(?1, ?2, OPTIONS => 'num_leaves_to_search: 200')",
				appender3.toString()
		);

		// 3 args expression
		StringBuilderSqlAppender appenderExpr = new StringBuilderSqlAppender();
		function.render( appenderExpr, List.of( node( "?1", appenderExpr ), node( "?2", appenderExpr ), expressionNode( "?3", appenderExpr ) ), null, walker );
		assertEquals(
				"APPROX_EUCLIDEAN_DISTANCE(?1, ?2, OPTIONS => ?3)",
				appenderExpr.toString()
		);
	}

	@Test
	@DisplayName("GoogleSQL: approx_dot_product with 2 and 3 arguments")
	public void testGoogleSqlApproxDotProduct() {
		SpannerGoogleSqlApproxDistanceFunction function = new SpannerGoogleSqlApproxDistanceFunction(
				"approx_dot_product",
				"APPROX_DOT_PRODUCT",
				typeConfiguration
		);

		// 2 args
		StringBuilderSqlAppender appender2 = new StringBuilderSqlAppender();
		function.render( appender2, List.of( node( "?1", appender2 ), node( "?2", appender2 ) ), null, walker );
		assertEquals(
				"APPROX_DOT_PRODUCT(?1, ?2, OPTIONS => 'num_leaves_to_search: 100')",
				appender2.toString()
		);

		// 3 args numeric
		StringBuilderSqlAppender appender3 = new StringBuilderSqlAppender();
		function.render( appender3, List.of( node( "?1", appender3 ), node( "?2", appender3 ), numericLiteral( 50 ) ), null, walker );
		assertEquals(
				"APPROX_DOT_PRODUCT(?1, ?2, OPTIONS => 'num_leaves_to_search: 50')",
				appender3.toString()
		);
	}

	// ------------------------------------------------------------------------
	// Spanner PostgreSQL Dialect Tests
	// ------------------------------------------------------------------------

	@Test
	@DisplayName("Spanner PostgreSQL: approx_cosine_distance with 2 arguments renders default json options")
	public void testPostgreSqlApproxCosineDistance2Args() {
		SpannerPostgreSqlApproxDistanceFunction function = new SpannerPostgreSqlApproxDistanceFunction(
				"approx_cosine_distance",
				"approx_cosine_distance",
				typeConfiguration
		);

		StringBuilderSqlAppender appender = new StringBuilderSqlAppender();
		List<SqlAstNode> args = List.of( node( "?1", appender ), node( "?2", appender ) );

		function.render( appender, args, null, walker );
		assertEquals(
				"spanner.approx_cosine_distance(?1, ?2, options => '{\"num_leaves_to_search\": 100}')",
				appender.toString()
		);
	}

	@Test
	@DisplayName("Spanner PostgreSQL: approx_cosine_distance with 3 arguments (integer literal) renders json options")
	public void testPostgreSqlApproxCosineDistance3ArgsNumeric() {
		SpannerPostgreSqlApproxDistanceFunction function = new SpannerPostgreSqlApproxDistanceFunction(
				"approx_cosine_distance",
				"spanner.approx_cosine_distance",
				typeConfiguration
		);

		StringBuilderSqlAppender appender = new StringBuilderSqlAppender();
		List<SqlAstNode> args = List.of( node( "?1", appender ), node( "?2", appender ), numericLiteral( 150 ) );

		function.render( appender, args, null, walker );
		assertEquals(
				"spanner.approx_cosine_distance(?1, ?2, options => '{\"num_leaves_to_search\": 150}')",
				appender.toString()
		);
	}

	@Test
	@DisplayName("Spanner PostgreSQL: approx_cosine_distance with 3 arguments (string literal) renders options verbatim")
	public void testPostgreSqlApproxCosineDistance3ArgsString() {
		SpannerPostgreSqlApproxDistanceFunction function = new SpannerPostgreSqlApproxDistanceFunction(
				"approx_cosine_distance",
				"spanner.approx_cosine_distance",
				typeConfiguration
		);

		StringBuilderSqlAppender appender = new StringBuilderSqlAppender();
		List<SqlAstNode> args = List.of(
				node( "?1", appender ),
				node( "?2", appender ),
				stringLiteral( "'{\"num_leaves_to_search\": 250}'", appender )
		);

		function.render( appender, args, null, walker );
		assertEquals(
				"spanner.approx_cosine_distance(?1, ?2, options => '{\"num_leaves_to_search\": 250}')",
				appender.toString()
		);
	}

	@Test
	@DisplayName("Spanner PostgreSQL: approx_euclidean_distance with 2 and 3 arguments")
	public void testPostgreSqlApproxEuclideanDistance() {
		SpannerPostgreSqlApproxDistanceFunction function = new SpannerPostgreSqlApproxDistanceFunction(
				"approx_euclidean_distance",
				"approx_euclidean_distance",
				typeConfiguration
		);

		// 2 args
		StringBuilderSqlAppender appender2 = new StringBuilderSqlAppender();
		function.render( appender2, List.of( node( "?1", appender2 ), node( "?2", appender2 ) ), null, walker );
		assertEquals(
				"spanner.approx_euclidean_distance(?1, ?2, options => '{\"num_leaves_to_search\": 100}')",
				appender2.toString()
		);

		// 3 args numeric
		StringBuilderSqlAppender appender3 = new StringBuilderSqlAppender();
		function.render( appender3, List.of( node( "?1", appender3 ), node( "?2", appender3 ), numericLiteral( 75 ) ), null, walker );
		assertEquals(
				"spanner.approx_euclidean_distance(?1, ?2, options => '{\"num_leaves_to_search\": 75}')",
				appender3.toString()
		);

		// 3 args expression
		StringBuilderSqlAppender appenderExpr = new StringBuilderSqlAppender();
		function.render( appenderExpr, List.of( node( "?1", appenderExpr ), node( "?2", appenderExpr ), expressionNode( "?3", appenderExpr ) ), null, walker );
		assertEquals(
				"spanner.approx_euclidean_distance(?1, ?2, options => ?3)",
				appenderExpr.toString()
		);
	}

	@Test
	@DisplayName("Spanner PostgreSQL: approx_dot_product with 2 and 3 arguments")
	public void testPostgreSqlApproxDotProduct() {
		SpannerPostgreSqlApproxDistanceFunction function = new SpannerPostgreSqlApproxDistanceFunction(
				"approx_dot_product",
				"approx_dot_product",
				typeConfiguration
		);

		// 2 args
		StringBuilderSqlAppender appender2 = new StringBuilderSqlAppender();
		function.render( appender2, List.of( node( "?1", appender2 ), node( "?2", appender2 ) ), null, walker );
		assertEquals(
				"spanner.approx_dot_product(?1, ?2, options => '{\"num_leaves_to_search\": 100}')",
				appender2.toString()
		);

		// 3 args numeric
		StringBuilderSqlAppender appender3 = new StringBuilderSqlAppender();
		function.render( appender3, List.of( node( "?1", appender3 ), node( "?2", appender3 ), numericLiteral( 120 ) ), null, walker );
		assertEquals(
				"spanner.approx_dot_product(?1, ?2, options => '{\"num_leaves_to_search\": 120}')",
				appender3.toString()
		);
	}

	// ------------------------------------------------------------------------
	// Type and Argument Resolution Tests
	// ------------------------------------------------------------------------

	@Test
	@DisplayName("Return type resolver resolves Double for both dialects")
	public void testReturnTypeResolution() {
		BasicType<Double> expectedDouble = typeConfiguration.getBasicTypeRegistry().resolve( StandardBasicTypes.DOUBLE );

		SpannerGoogleSqlApproxDistanceFunction googleSql = new SpannerGoogleSqlApproxDistanceFunction(
				"approx_cosine_distance",
				typeConfiguration
		);
		BasicType<?> googleSqlRet = (BasicType<?>) googleSql.getReturnTypeResolver()
				.resolveFunctionReturnType( null, (SqmToSqlAstConverter) null, null, typeConfiguration );
		assertEquals( expectedDouble, googleSqlRet );

		SpannerPostgreSqlApproxDistanceFunction pgSql = new SpannerPostgreSqlApproxDistanceFunction(
				"approx_cosine_distance",
				typeConfiguration
		);
		BasicType<?> pgRet = (BasicType<?>) pgSql.getReturnTypeResolver()
				.resolveFunctionReturnType( null, (SqmToSqlAstConverter) null, null, typeConfiguration );
		assertEquals( expectedDouble, pgRet );
	}

	@Test
	@DisplayName("Argument type resolver resolves null for 3rd argument (options), preventing vector typing")
	public void testArgumentTypeResolutionExcludesOptionsArgument() {
		SpannerGoogleSqlApproxDistanceFunction googleSql = new SpannerGoogleSqlApproxDistanceFunction(
				"approx_cosine_distance",
				typeConfiguration
		);

		FunctionArgumentTypeResolver resolver = googleSql.getArgumentTypeResolver();
		assertNotNull( resolver );

		SqmToSqlAstConverter converter = mock( SqmToSqlAstConverter.class );
		org.hibernate.sql.ast.spi.creation.SqlAstCreationContext creationContext =
				mock( org.hibernate.sql.ast.spi.creation.SqlAstCreationContext.class );
		when( converter.getCreationContext() ).thenReturn( creationContext );
		when( creationContext.getTypeConfiguration() ).thenReturn( typeConfiguration );

		SqmTypedNode<?> arg1 = mock( SqmTypedNode.class );
		SqmTypedNode<?> arg2 = mock( SqmTypedNode.class );
		SqmTypedNode<?> arg3 = mock( SqmTypedNode.class );
		List<SqmTypedNode<?>> args = List.of( arg1, arg2, arg3 );

		// Argument 2 (options) MUST resolve to null
		MappingModelExpressible<?> optionsType = resolver.resolveFunctionArgumentType( args, 2, converter );
		assertNull( optionsType, "Options argument (index 2) must not be inferred as vector" );
	}
}
