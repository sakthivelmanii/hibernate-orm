/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.dialect.sql.ast;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.sql.ast.tree.Statement;
import org.hibernate.sql.ast.tree.cte.CteMaterialization;
import org.hibernate.sql.ast.tree.predicate.LikePredicate;
import org.hibernate.sql.exec.spi.JdbcOperation;

public class SpannerPostgreSQLSqlAstTranslator<T extends JdbcOperation>
		extends PostgreSQLSqlAstTranslator<T> {

	public SpannerPostgreSQLSqlAstTranslator(
			SessionFactoryImplementor sessionFactory, Statement statement) {
		super(sessionFactory, statement);
	}

	@Override
	protected void renderMaterializationHint(CteMaterialization materialization) {
		// NO-OP
	}

	@Override
	public void visitLikePredicate(LikePredicate likePredicate) {
		likePredicate.getMatchExpression().accept( this );
		if ( likePredicate.isNegated() ) {
			appendSql( " not" );
		}
		if ( likePredicate.isCaseSensitive() ) {
			appendSql( " like " );
		}
		else {
			appendSql( WHITESPACE );
			appendSql( getDialect().getCaseInsensitiveLike() );
			appendSql( WHITESPACE );
		}
		likePredicate.getPattern().accept( this );
		if ( likePredicate.getEscapeCharacter() != null ) {
			appendSql( " escape " );
			likePredicate.getEscapeCharacter().accept( this );
		}
	}
}
