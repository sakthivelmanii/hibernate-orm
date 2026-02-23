/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.community.dialect.function;

import java.util.List;

import org.hibernate.dialect.function.json.PostgreSQLJsonObjectFunction;
import org.hibernate.metamodel.model.domain.ReturnableType;
import org.hibernate.sql.ast.SqlAstTranslator;
import org.hibernate.sql.ast.spi.SqlAppender;
import org.hibernate.sql.ast.tree.SqlAstNode;
import org.hibernate.sql.ast.tree.expression.JsonNullBehavior;
import org.hibernate.type.spi.TypeConfiguration;

/**
 * Spanner specific PostgreSQL json_object function.
 */
public class SpannerPostgreSQLJsonObjectFunction extends PostgreSQLJsonObjectFunction {

	public SpannerPostgreSQLJsonObjectFunction(TypeConfiguration typeConfiguration) {
		super( typeConfiguration );
	}

	@Override
	public void render(
			SqlAppender sqlAppender,
			List<? extends SqlAstNode> sqlAstArguments,
			ReturnableType<?> returnType,
			SqlAstTranslator<?> walker) {
		if ( sqlAstArguments.isEmpty() ) {
			sqlAppender.appendSql( "jsonb_build_object()" );
		}
		else {
			final SqlAstNode lastArgument = sqlAstArguments.get( sqlAstArguments.size() - 1 );
			final JsonNullBehavior nullBehavior;
			final int argumentsCount;
			if ( lastArgument instanceof JsonNullBehavior jsonNullBehavior ) {
				nullBehavior = jsonNullBehavior;
				argumentsCount = sqlAstArguments.size() - 1;
			}
			else {
				nullBehavior = JsonNullBehavior.NULL;
				argumentsCount = sqlAstArguments.size();
			}
			if ( nullBehavior == JsonNullBehavior.ABSENT ) {
				boolean first = true;
				sqlAppender.appendSql( "(" );
				for ( int i = 0; i < argumentsCount; i += 2 ) {
					final SqlAstNode key = sqlAstArguments.get( i );
					final SqlAstNode value = sqlAstArguments.get( i + 1 );
					if ( !first ) {
						sqlAppender.appendSql( " || " );
					}
					sqlAppender.appendSql( "(case when " );
					value.accept( walker );
					sqlAppender.appendSql( " is not null then jsonb_build_object(" );
					key.accept( walker );
					sqlAppender.appendSql( "," );
					value.accept( walker );
					sqlAppender.appendSql( ") else '{}'::jsonb end)" );
					first = false;
				}
				sqlAppender.appendSql( ")" );
			}
			else {
				super.render( sqlAppender, sqlAstArguments, returnType, walker );
			}
		}
	}
}
