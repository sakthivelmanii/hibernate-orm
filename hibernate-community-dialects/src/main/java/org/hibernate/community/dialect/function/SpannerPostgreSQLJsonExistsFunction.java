/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.community.dialect.function;

import java.util.Map;

import org.hibernate.QueryException;
import org.hibernate.dialect.function.json.PostgreSQLJsonExistsFunction;
import org.hibernate.metamodel.model.domain.ReturnableType;
import org.hibernate.sql.ast.SqlAstTranslator;
import org.hibernate.sql.ast.spi.SqlAppender;
import org.hibernate.sql.ast.tree.expression.Expression;
import org.hibernate.sql.ast.tree.expression.JsonExistsErrorBehavior;
import org.hibernate.sql.ast.tree.expression.JsonPathPassingClause;
import org.hibernate.type.spi.TypeConfiguration;

public class SpannerPostgreSQLJsonExistsFunction extends PostgreSQLJsonExistsFunction {

	public SpannerPostgreSQLJsonExistsFunction(TypeConfiguration typeConfiguration) {
		super(typeConfiguration);
	}

	@Override
	protected void render(
			SqlAppender sqlAppender,
			JsonExistsArguments arguments,
			ReturnableType<?> returnType,
			SqlAstTranslator<?> walker) {
		// jsonb_path_exists errors by default
		if (arguments.errorBehavior() != null && arguments.errorBehavior() != JsonExistsErrorBehavior.ERROR) {
			throw new QueryException("Can't emulate on error clause on PostgreSQL");
		}
		appendJsonExists(sqlAppender, walker, arguments.jsonDocument(), arguments.jsonPath(), arguments.passingClause());
	}

	static void appendJsonExists(SqlAppender sqlAppender, SqlAstTranslator<?> walker, Expression jsonDocument,
			Expression jsonPath, JsonPathPassingClause passingClause) {
		sqlAppender.appendSql("jsonb_path_exists(");
		jsonDocument.accept(walker);
		sqlAppender.appendSql(',');
		jsonPath.accept(walker);
		sqlAppender.appendSql("::jsonpath");
		if (passingClause != null) {
			sqlAppender.append(",jsonb_build_object");
			char separator = '(';
			for (Map.Entry<String, Expression> entry : passingClause.getPassingExpressions().entrySet()) {
				sqlAppender.append(separator);
				sqlAppender.appendSingleQuoteEscapedString(entry.getKey());
				sqlAppender.append(',');
				entry.getValue().accept(walker);
				separator = ',';
			}
			sqlAppender.append(')');
		}
		sqlAppender.appendSql(')');
	}
}
