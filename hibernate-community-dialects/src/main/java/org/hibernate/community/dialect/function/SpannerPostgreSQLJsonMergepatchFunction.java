/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.community.dialect.function;

import java.util.List;

import org.hibernate.dialect.function.json.PostgreSQLJsonMergepatchFunction;
import org.hibernate.metamodel.mapping.JdbcMappingContainer;
import org.hibernate.metamodel.model.domain.ReturnableType;
import org.hibernate.sql.ast.SqlAstTranslator;
import org.hibernate.sql.ast.spi.SqlAppender;
import org.hibernate.sql.ast.tree.SqlAstNode;
import org.hibernate.sql.ast.tree.expression.Expression;
import org.hibernate.type.spi.TypeConfiguration;

/**
 * Spanner PostgreSQL json_mergepatch function.
 * <p>
 * NOTE: Spanner PostgreSQL does not support recursive CTEs in subqueries, which
 * are required for a full
 * RFC 7396 compliant deep merge. This implementation uses the concatenation
 * operator ({@code ||})
 * which performs a shallow merge. This is sufficient for basic use cases but
 * will not merge nested objects correctly
 * (it replaces them).
 */
public class SpannerPostgreSQLJsonMergepatchFunction extends PostgreSQLJsonMergepatchFunction {

	public SpannerPostgreSQLJsonMergepatchFunction(TypeConfiguration typeConfiguration) {
		super(typeConfiguration);
	}

	@Override
	public void render(
			SqlAppender sqlAppender,
			List<? extends SqlAstNode> arguments,
			ReturnableType<?> returnType,
			SqlAstTranslator<?> translator) {
		// Spanner PostgreSQL does not support recursive CTEs in subqueries needed for
		// deep merge.
		// We use || operator for a shallow merge as a best-effort implementation.
		sqlAppender.appendSql("(");
		for (int i = 0; i < arguments.size(); i++) {
			if (i > 0) {
				sqlAppender.appendSql(" || ");
			}
			renderJsonDocumentExpression(sqlAppender, translator, (Expression) arguments.get(i));
		}
		sqlAppender.appendSql(")");
	}

	private void renderJsonDocumentExpression(SqlAppender sqlAppender, SqlAstTranslator<?> translator, Expression json) {
		final boolean needsCast = !isJsonType(json);
		if (needsCast) {
			sqlAppender.appendSql("cast(");
		}
		json.accept(translator);
		if (needsCast) {
			sqlAppender.appendSql(" as jsonb)");
		}
	}

	private boolean isJsonType(Expression expression) {
		final JdbcMappingContainer expressionType = expression.getExpressionType();
		return expressionType != null && expressionType.getSingleJdbcMapping().getJdbcType().isJson();
	}
}
