/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.community.dialect.function;

import java.util.List;

import org.hibernate.dialect.function.json.JsonArrayAggFunction;
import org.hibernate.metamodel.model.domain.ReturnableType;
import org.hibernate.sql.ast.Clause;
import org.hibernate.sql.ast.SqlAstTranslator;
import org.hibernate.sql.ast.spi.SqlAppender;
import org.hibernate.sql.ast.tree.SqlAstNode;
import org.hibernate.sql.ast.tree.expression.Distinct;
import org.hibernate.sql.ast.tree.expression.Expression;
import org.hibernate.sql.ast.tree.expression.JsonNullBehavior;
import org.hibernate.sql.ast.tree.predicate.Predicate;
import org.hibernate.sql.ast.tree.select.SortSpecification;
import org.hibernate.type.spi.TypeConfiguration;

/**
 * Spanner PostgreSQL json_arrayagg function.
 */
public class SpannerPostgreSQLJsonArrayAggFunction extends JsonArrayAggFunction {

	public SpannerPostgreSQLJsonArrayAggFunction(TypeConfiguration typeConfiguration) {
		super(true, typeConfiguration);
	}

	@Override
	public void render(
			SqlAppender sqlAppender,
			List<? extends SqlAstNode> sqlAstArguments,
			Predicate filter,
			List<SortSpecification> withinGroup,
			ReturnableType<?> returnType,
			SqlAstTranslator<?> translator) {
		// Spanner PostgreSQL does not support jsonb_agg, FILTER clause on aggregates,
		// or array_remove.
		// Workaround: Use string_agg with manual JSON formatting and CASE for
		// filtering/null-handling.
		// Formula: coalesce('[' || string_agg(DISTINCT CASE WHEN filter THEN
		// transform(arg) END, ',' ORDER BY ...) || ']', '[]')::jsonb

		final JsonNullBehavior nullBehavior;
		if (sqlAstArguments.size() > 1) {
			nullBehavior = (JsonNullBehavior) sqlAstArguments.get(1);
		} else {
			nullBehavior = JsonNullBehavior.ABSENT;
		}

		sqlAppender.appendSql("coalesce('[' || string_agg(");

		final SqlAstNode firstArg = sqlAstArguments.get(0);
		final Expression argExpression;
		if (firstArg instanceof Distinct distinct) {
			sqlAppender.appendSql("distinct ");
			argExpression = distinct.getExpression();
		} else {
			argExpression = (Expression) firstArg;
		}

		// CASE WHEN filter ...
		if (filter != null) {
			sqlAppender.appendSql("case when ");
			filter.accept(translator);
			sqlAppender.appendSql(" then ");
		}

		// Argument transformation
		// ABSENT ON NULL: to_jsonb(arg)::text (result is NULL if arg is NULL, so
		// string_agg skips it)
		// NULL ON NULL: coalesce(to_jsonb(arg)::text, 'null')
		if (nullBehavior == JsonNullBehavior.ABSENT) {
			sqlAppender.appendSql("to_jsonb(");
			argExpression.accept(translator);
			sqlAppender.appendSql(")::text");
		} else {
			sqlAppender.appendSql("coalesce(to_jsonb(");
			argExpression.accept(translator);
			sqlAppender.appendSql(")::text, 'null')");
		}

		if (filter != null) {
			sqlAppender.appendSql(" end");
		}

		sqlAppender.appendSql(", ','");

		if (withinGroup != null && !withinGroup.isEmpty()) {
			translator.getCurrentClauseStack().push(Clause.WITHIN_GROUP);
			sqlAppender.appendSql(" order by ");
			withinGroup.get(0).accept(translator);
			for (int i = 1; i < withinGroup.size(); i++) {
				sqlAppender.appendSql(',');
				withinGroup.get(i).accept(translator);
			}
			translator.getCurrentClauseStack().pop();
		}

		sqlAppender.appendSql(") || ']', '[]')::jsonb");
	}
}
