/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.community.dialect.function;

import java.util.List;
import java.util.Map;

import org.hibernate.QueryException;
import org.hibernate.dialect.function.json.JsonPathHelper;
import org.hibernate.dialect.function.json.PostgreSQLJsonValueFunction;
import org.hibernate.metamodel.mapping.JdbcMappingContainer;
import org.hibernate.metamodel.model.domain.ReturnableType;
import org.hibernate.sql.ast.SqlAstTranslator;
import org.hibernate.sql.ast.spi.SqlAppender;
import org.hibernate.sql.ast.tree.expression.Expression;
import org.hibernate.type.spi.TypeConfiguration;

/**
 * Spanner PostgreSQL json_value function.
 * <p>
 * This implementation avoids using {@code jsonb_path_query_first} which is not supported in Spanner.
 * Instead, it constructs the extraction using nested {@code ->} and {@code ->>} operators.
 */
public class SpannerPostgreSQLJsonValueFunction extends PostgreSQLJsonValueFunction {

	public SpannerPostgreSQLJsonValueFunction(TypeConfiguration typeConfiguration) {
		super( false, typeConfiguration );
	}

	@Override
	protected void render(
			SqlAppender sqlAppender,
			JsonValueArguments arguments,
			ReturnableType<?> returnType,
			SqlAstTranslator<?> walker) {

		// Use default render logic for arguments validation if needed, but we essentially override the main generation via custom logic
		// simpler to just implement the extraction directly here.

		if ( arguments.returningType() != null ) {
			sqlAppender.appendSql( "cast(" );
		}

		final Expression json = arguments.jsonDocument();
		final String pathLiteral = walker.getLiteralValue( arguments.jsonPath() );
		final List<JsonPathHelper.JsonPathElement> jsonPathElements = JsonPathHelper.parseJsonPathElements( pathLiteral );
		final Map<String, Expression> passingMap = arguments.passingClause() != null ? arguments.passingClause().getPassingExpressions() : Map.of();

		// Generate: json -> 'p1' -> 'p2' ->> 'p3'

		// Cast input to jsonb if needed
		boolean needsCast = !isJsonType( json );
		sqlAppender.appendSql( "(" );
		if ( needsCast ) {
			sqlAppender.appendSql( "cast(" );
		}
		json.accept( walker );
		if ( needsCast ) {
			sqlAppender.appendSql( " as jsonb)" );
		}

		for ( int i = 0; i < jsonPathElements.size(); i++ ) {
			JsonPathHelper.JsonPathElement element = jsonPathElements.get( i );
			boolean isLast = ( i == jsonPathElements.size() - 1 );
			// Use ->> for the last element to get text, -> for others to get jsonb
			sqlAppender.appendSql( isLast ? "->>" : "->" );

			if ( element instanceof JsonPathHelper.JsonAttribute attribute ) {
				sqlAppender.appendSingleQuoteEscapedString( attribute.attribute() );
			}
			else if ( element instanceof JsonPathHelper.JsonIndexAccess indexAccess ) {
				sqlAppender.appendSql( Integer.toString( indexAccess.index() ) );
			}
			else if ( element instanceof JsonPathHelper.JsonParameterIndexAccess paramAccess ) {
				Expression paramExpr = passingMap.get( paramAccess.parameterName() );
				if ( paramExpr == null ) {
					throw new QueryException( "Missing parameter [" + paramAccess.parameterName() + "] for path [" + pathLiteral + "]" );
				}
				// The parameter expression (e.g. integer) needs to be rendered
				// If it's a literal number, we can render it directly?
				// -> operator takes integer or text.
				// If paramExpr is a bound parameter `?`, passing it to `->` usually works if type is inferred or set?
				// But `->` operator in PG expects integer for array access and text for object access.
				// If it's `JsonParameterIndexAccess` it implies array index usually?
				// Actually `JsonPathHelper` distinguishes `JsonAttribute` vs `JsonIndexAccess` vs `JsonParameterIndexAccess`.
				// `JsonParameterIndexAccess` is usually an index (e.g. `[ $idx ]`).
				// So we assume it calculates an integer.

				sqlAppender.appendSql( "(" );
				paramExpr.accept( walker );
				sqlAppender.appendSql( ")" );
			}
			else {
				// Should not happen with current helper
				throw new QueryException( "Unknown path element type: " + element.getClass() );
			}
		}

		sqlAppender.appendSql( ")" ); // Close parenthesis for the whole extraction

		if ( arguments.returningType() != null ) {
			sqlAppender.appendSql( " as " );
			arguments.returningType().accept( walker );
			sqlAppender.appendSql( ")" );
		}
	}

	private static boolean isJsonType(Expression expression) {
		final JdbcMappingContainer expressionType = expression.getExpressionType();
		return expressionType != null && expressionType.getSingleJdbcMapping().getJdbcType().isJson();
	}

}
