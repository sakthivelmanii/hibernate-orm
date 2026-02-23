/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.community.dialect.function;

import java.util.List;

import org.hibernate.QueryException;
import org.hibernate.dialect.function.json.JsonPathHelper;
import org.hibernate.dialect.function.json.PostgreSQLJsonArrayAppendFunction;
import org.hibernate.metamodel.mapping.JdbcMappingContainer;
import org.hibernate.metamodel.model.domain.ReturnableType;
import org.hibernate.sql.ast.SqlAstTranslator;
import org.hibernate.sql.ast.spi.SqlAppender;
import org.hibernate.sql.ast.tree.SqlAstNode;
import org.hibernate.sql.ast.tree.expression.Expression;
import org.hibernate.sql.ast.tree.expression.Literal;
import org.hibernate.type.spi.TypeConfiguration;

/**
 * Spanner PostgreSQL json_array_append function.
 * <p>
 * This implementation avoids using {@code FROM (VALUES(...))} which is not
 * supported in Spanner subqueries.
 * Instead, it constructs the path array inline.
 * <p>
 * It also avoids {@code #>} operator with text array, as Spanner PG doesn't
 * support {@code jsonb_extract_path}.
 * Instead, it generates nested {@code ->} operators for path extraction.
 */
public class SpannerPostgreSQLJsonArrayAppendFunction extends PostgreSQLJsonArrayAppendFunction {

	private final boolean supportsLax;

	public SpannerPostgreSQLJsonArrayAppendFunction(boolean supportsLax, TypeConfiguration typeConfiguration) {
		super(supportsLax, typeConfiguration);
		this.supportsLax = supportsLax;
	}

	@Override
	public void render(
			SqlAppender sqlAppender,
			List<? extends SqlAstNode> arguments,
			ReturnableType<?> returnType,
			SqlAstTranslator<?> translator) {
		final Expression json = (Expression) arguments.get(0);
		final Expression jsonPath = (Expression) arguments.get(1);
		final SqlAstNode value = arguments.get(2);

		String pathLiteral = translator.getLiteralValue(jsonPath);
		List<JsonPathHelper.JsonPathElement> jsonPathElements = JsonPathHelper.parseJsonPathElements(pathLiteral);

		// Helper to render the JSON expression with casting if needed
		Runnable renderJson = () -> {
			final boolean needsCast = !isJsonType(json);
			if (needsCast) {
				sqlAppender.appendSql("cast(");
			}
			json.accept(translator);
			if (needsCast) {
				sqlAppender.appendSql(" as jsonb)");
			}
		};

		// Helper to render path array: array['a', 'b']
		Runnable renderPathArray = () -> {
			sqlAppender.appendSql("array[");
			boolean first = true;
			for (JsonPathHelper.JsonPathElement pathElement : jsonPathElements) {
				if (!first) {
					sqlAppender.appendSql(",");
				}
				first = false;
				if (pathElement instanceof JsonPathHelper.JsonAttribute attribute) {
					sqlAppender.appendSingleQuoteEscapedString(attribute.attribute());
				} else if (pathElement instanceof JsonPathHelper.JsonParameterIndexAccess param) {
					throw new QueryException("JSON path [" + pathLiteral + "] uses parameter [" + param.parameterName()
							+ "] which is not supported in this dialect combination.");
				} else {
					sqlAppender.appendSql("'");
					sqlAppender.appendSql(((JsonPathHelper.JsonIndexAccess) pathElement).index() + 1);
					sqlAppender.appendSql("'");
				}
			}
			// Spanner PG requires text[] instead of character varying[]
			sqlAppender.appendSql("]::text[]");
		};

		// Helper to render extraction: json -> 'a' -> 'b'
		Runnable renderExtraction = () -> {
			sqlAppender.appendSql("(");
			renderJson.run();
			for (JsonPathHelper.JsonPathElement pathElement : jsonPathElements) {
				sqlAppender.appendSql("->");
				if (pathElement instanceof JsonPathHelper.JsonAttribute attribute) {
					sqlAppender.appendSingleQuoteEscapedString(attribute.attribute());
				} else if (pathElement instanceof JsonPathHelper.JsonParameterIndexAccess param) {
					throw new QueryException("Unsupported parameter");
				} else {
					sqlAppender.appendSql(Integer.toString(((JsonPathHelper.JsonIndexAccess) pathElement).index()));
				}
			}
			sqlAppender.appendSql(")");
		};

		sqlAppender.appendSql("(select ");
		if (supportsLax) {
			sqlAppender.appendSql("jsonb_set_lax(");
			renderJson.run();
			sqlAppender.appendSql(",");
			renderPathArray.run();
			sqlAppender.appendSql(",");
			// json -> path || new_value
			renderExtraction.run();
			sqlAppender.appendSql("||");
			renderValue(sqlAppender, value, translator);
			sqlAppender.appendSql(",false,'return_target')");
		} else {
			sqlAppender.appendSql("case when ");
			renderExtraction.run();
			sqlAppender.appendSql(" is not null then jsonb_set(");
			renderJson.run();
			sqlAppender.appendSql(",");
			renderPathArray.run();
			sqlAppender.appendSql(",");
			// json -> path || new_value
			renderExtraction.run();
			sqlAppender.appendSql("||");
			renderValue(sqlAppender, value, translator);
			sqlAppender.appendSql(",false) else ");
			renderJson.run();
			sqlAppender.appendSql(" end");
		}
		sqlAppender.appendSql(")");
	}

	private void renderValue(SqlAppender sqlAppender, SqlAstNode value, SqlAstTranslator<?> translator) {
		if (value instanceof Literal literal && literal.getLiteralValue() == null) {
			sqlAppender.appendSql("null::jsonb");
		} else {
			sqlAppender.appendSql("to_jsonb(");
			value.accept(translator);
			if (value instanceof Literal literal && literal.getJdbcMapping().getJdbcType().isString()) {
				// PostgreSQL until version 16 is not smart enough to infer the type of a string
				// literal
				sqlAppender.appendSql("::text");
			}
			sqlAppender.appendSql(')');
		}
	}

	private static boolean isJsonType(Expression expression) {
		final JdbcMappingContainer expressionType = expression.getExpressionType();
		return expressionType != null && expressionType.getSingleJdbcMapping().getJdbcType().isJson();
	}
}
