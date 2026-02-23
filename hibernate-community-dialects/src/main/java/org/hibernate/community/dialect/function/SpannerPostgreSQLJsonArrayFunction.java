/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.community.dialect.function;

import java.util.List;

import org.hibernate.dialect.function.json.JsonArrayFunction;
import org.hibernate.metamodel.model.domain.ReturnableType;
import org.hibernate.sql.ast.SqlAstTranslator;
import org.hibernate.sql.ast.spi.SqlAppender;
import org.hibernate.sql.ast.tree.SqlAstNode;
import org.hibernate.sql.ast.tree.expression.JsonNullBehavior;
import org.hibernate.type.spi.TypeConfiguration;

/**
 * Spanner PostgreSQL json_array function.
 */
public class SpannerPostgreSQLJsonArrayFunction extends JsonArrayFunction {

	public SpannerPostgreSQLJsonArrayFunction(TypeConfiguration typeConfiguration) {
		super(typeConfiguration);
	}

	@Override
	public void render(
			SqlAppender sqlAppender,
			List<? extends SqlAstNode> sqlAstArguments,
			ReturnableType<?> returnType,
			SqlAstTranslator<?> walker) {
		if (sqlAstArguments.isEmpty()) {
			sqlAppender.appendSql("jsonb_build_array()");
		} else {
			final SqlAstNode lastArgument = sqlAstArguments.get(sqlAstArguments.size() - 1);
			final JsonNullBehavior nullBehavior;
			final int argumentsCount;
			if (lastArgument instanceof JsonNullBehavior jsonNullBehavior) {
				nullBehavior = jsonNullBehavior;
				argumentsCount = sqlAstArguments.size() - 1;
			} else {
				// Default to ABSENT ON NULL as per standard/Hibernate expectation if not
				// specified?
				// Actually JsonArrayFunction default logic:
				// if last arg is not behavior, it assumes defaults or caller handles it?
				// JsonArrayFunction.render says:
				// else { nullBehavior = JsonNullBehavior.ABSENT; argumentsCount = size; }
				nullBehavior = JsonNullBehavior.ABSENT;
				argumentsCount = sqlAstArguments.size();
			}

			if (nullBehavior == JsonNullBehavior.ABSENT) {
				// Spanner PG doesn't support the complex VALUES subquery used by
				// PostgreSQLJsonArrayFunction well.
				// We use concatenation of arrays:
				// (CASE WHEN arg1 IS NOT NULL THEN jsonb_build_array(arg1) ELSE '[]'::jsonb
				// END) || ...

				// However, we must ensure at least one argument or handle empty case (already
				// handled above).
				// Also, we need to handle precedence/parentheses if needed, but || associates
				// left-to-right.

				// Correction: '[]'::jsonb is strictly empty array.
				// jsonb_build_array(arg) creates [arg].
				// [arg] || [] -> [arg].

				// We wrap the whole thing in parens just in case.
				sqlAppender.appendSql("(");

				for (int i = 0; i < argumentsCount; i++) {
					if (i > 0) {
						sqlAppender.appendSql(" || ");
					}
					sqlAppender.appendSql("(case when ");
					sqlAstArguments.get(i).accept(walker);
					sqlAppender.appendSql(" is not null then jsonb_build_array(");
					sqlAstArguments.get(i).accept(walker);
					sqlAppender.appendSql(") else '[]'::jsonb end)");
				}

				sqlAppender.appendSql(")");
			} else {
				// NULL ON NULL -> jsonb_build_array includes nulls by default.
				sqlAppender.appendSql("jsonb_build_array(");
				for (int i = 0; i < argumentsCount; i++) {
					if (i > 0) {
						sqlAppender.appendSql(", ");
					}
					sqlAstArguments.get(i).accept(walker);
				}
				sqlAppender.appendSql(")");
			}
		}
	}
}
