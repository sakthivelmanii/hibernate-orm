/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.dialect.aggregate;

import org.hibernate.mapping.Column;
import org.hibernate.metamodel.mapping.SelectableMapping;
import org.hibernate.metamodel.mapping.SqlTypedMapping;
import org.hibernate.type.spi.TypeConfiguration;

public class SpannerPostgreSQLAggregateSupport extends PostgreSQLAggregateSupport {

	public static final AggregateSupport INSTANCE = new SpannerPostgreSQLAggregateSupport();

	@Override
	public String aggregateComponentCustomReadExpression(String template, String placeholder, String aggregateParentReadExpression, String columnExpression, int aggregateColumnTypeCode, SqlTypedMapping column, TypeConfiguration typeConfiguration) {
		throw new IllegalArgumentException( "Unsupported aggregate SQL type: " + aggregateColumnTypeCode );
	}

	@Override
	public String aggregateComponentAssignmentExpression(String aggregateParentAssignmentExpression, String columnExpression, int aggregateColumnTypeCode, Column column) {
		throw new IllegalArgumentException( "Unsupported aggregate SQL type: " + aggregateColumnTypeCode );
	}

	@Override
	public boolean requiresAggregateCustomWriteExpressionRenderer(int aggregateSqlTypeCode) {
		return false;
	}

	@Override
	public WriteExpressionRenderer aggregateCustomWriteExpressionRenderer(SelectableMapping aggregateColumn, SelectableMapping[] columnsToUpdate, TypeConfiguration typeConfiguration) {
		final int aggregateSqlTypeCode = aggregateColumn.getJdbcMapping().getJdbcType().getDefaultSqlTypeCode();
		throw new IllegalArgumentException( "Unsupported aggregate SQL type: " + aggregateSqlTypeCode );
	}
}
