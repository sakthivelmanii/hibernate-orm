/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.community.dialect.function;

import java.util.List;

import org.hibernate.dialect.function.array.AbstractArrayIntersectsFunction;
import org.hibernate.dialect.function.array.DdlTypeHelper;
import org.hibernate.metamodel.mapping.JdbcMappingContainer;
import org.hibernate.metamodel.model.domain.ReturnableType;
import org.hibernate.sql.ast.SqlAstTranslator;
import org.hibernate.sql.ast.spi.SqlAppender;
import org.hibernate.sql.ast.tree.SqlAstNode;
import org.hibernate.sql.ast.tree.expression.Expression;
import org.hibernate.sql.ast.tree.expression.FunctionExpression;
import org.hibernate.type.BasicPluralType;
import org.hibernate.type.spi.TypeConfiguration;

/**
 * Spanner specific array_intersects function.
 */
public class SpannerArrayIntersectsFunction extends AbstractArrayIntersectsFunction {

	public SpannerArrayIntersectsFunction(boolean nullable, TypeConfiguration typeConfiguration) {
		super(nullable, typeConfiguration);
	}

	@Override
	public void render(
			SqlAppender sqlAppender,
			List<? extends SqlAstNode> sqlAstArguments,
			ReturnableType<?> returnType,
			SqlAstTranslator<?> walker) {
		final Expression haystackExpression = (Expression) sqlAstArguments.get(0);
		final Expression needleExpression = (Expression) sqlAstArguments.get(1);

		renderIntersects(sqlAppender, haystackExpression, needleExpression, walker);
	}

	private void renderIntersects(
			SqlAppender sqlAppender,
			Expression haystackExpression,
			Expression needleExpression,
			SqlAstTranslator<?> walker) {
		haystackExpression.accept(walker);
		sqlAppender.append(" && ");
		if (needsArrayCasting(needleExpression)) {
			String castTypeName = DdlTypeHelper.getCastTypeName(
					haystackExpression.getExpressionType(),
					walker.getSessionFactory().getTypeConfiguration());
			if (castTypeName.toLowerCase().contains("character varying[]") || castTypeName.toLowerCase()
					.contains("character varying array")) {
				// Spanner PG requires text[] instead of character varying[]
				castTypeName = "text[]";
			}

			// Avoid double casting if the argument is already a cast
			Expression renderExpression = needleExpression;
			if (needleExpression instanceof FunctionExpression) {
				final FunctionExpression functionExpression = (FunctionExpression) needleExpression;
				if ("cast".equalsIgnoreCase(functionExpression.getFunctionName())) {
					renderExpression = (Expression) functionExpression.getArguments().get(0);
				}
			}

			sqlAppender.append("cast(");
			renderExpression.accept(walker);
			sqlAppender.append(" as ");
			sqlAppender.append(castTypeName);
			sqlAppender.append(')');
		} else {
			needleExpression.accept(walker);
		}
	}

	private static boolean needsArrayCasting(Expression expression) {
		JdbcMappingContainer typeContainer = expression.getExpressionType();
		if (typeContainer instanceof BasicPluralType) {
			return ((BasicPluralType<?, ?>) typeContainer).getElementType().getJdbcType().isString();
		}
		return false;
	}
}
