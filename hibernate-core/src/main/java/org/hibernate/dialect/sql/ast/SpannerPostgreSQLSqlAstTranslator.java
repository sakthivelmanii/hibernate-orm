/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.dialect.sql.ast;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.sql.ast.tree.Statement;
import org.hibernate.sql.ast.tree.predicate.ExistsPredicate;
import org.hibernate.sql.ast.tree.predicate.LikePredicate;
import org.hibernate.sql.exec.spi.JdbcOperation;

public class SpannerPostgreSQLSqlAstTranslator<T extends JdbcOperation>
		extends PostgreSQLSqlAstTranslator<T> {

	public SpannerPostgreSQLSqlAstTranslator(
			SessionFactoryImplementor sessionFactory, Statement statement) {
		super(sessionFactory, statement);
	}

	@Override
	public void visitExistsPredicate(ExistsPredicate existsPredicate) {
		existsPredicate.accept( this );
	}

	@Override
	public void visitLikePredicate(LikePredicate likePredicate) {
		// Since Spanner doesn't support escape character, we are skipping escape character
		likePredicate.getMatchExpression().accept(this);
		if (likePredicate.isNegated()) {
			appendSql(" not");
		}
		if (likePredicate.isCaseSensitive()) {
			appendSql(" like ");
		} else {
			appendSql(WHITESPACE);
			appendSql(getDialect().getCaseInsensitiveLike());
			appendSql(WHITESPACE);
		}
		likePredicate.getPattern().accept(this);
	}
}
