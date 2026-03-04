/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.dialect.function;

import org.hibernate.sql.ast.spi.SqlAppender;

public class SpannerPostgreSQLTruncRoundFunction extends PostgreSQLTruncRoundFunction{

	public SpannerPostgreSQLTruncRoundFunction(String name, boolean supportsTwoArguments) {
		super( name, supportsTwoArguments );
	}

	@Override
	protected void addTypeCasting(int numberOfArguments, SqlAppender sqlAppender) {
		if ( numberOfArguments == 1 ) {
			sqlAppender.append( "::float8" );
		}
	}
}
