/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.dialect.sequence;

import org.hibernate.MappingException;

public class SpannerPostgreSQLSequenceSupport implements SequenceSupport {

	public static final SpannerPostgreSQLSequenceSupport INSTANCE = new SpannerPostgreSQLSequenceSupport();

	@Override
	public String getCreateSequenceString(String sequenceName, int initialValue, int incrementSize) throws MappingException {
		if ( incrementSize == 0 ) {
			throw new MappingException( "Unable to create the sequence [" + sequenceName + "]: the increment size must not be 0" );
		}
		return getCreateSequenceString( sequenceName )
			+ startingValue( initialValue, incrementSize )
			+ " start counter with " + initialValue;
	}

	@Override
	public String getCreateSequenceString(String sequenceName) throws MappingException {
		return "create sequence " + sequenceName + " bit_reversed_positive";
	}

	@Override
	public String getSelectSequenceNextValString(String sequenceName) {
		return "nextval('" + sequenceName + "')";
	}

	@Override
	public String getSelectSequencePreviousValString(String sequenceName) throws MappingException {
		return "currval('" + sequenceName + "')";
	}

	@Override
	public boolean sometimesNeedsStartingValue() {
		return true;
	}

	@Override
	public String getDropSequenceString(String sequenceName) {
		return "drop sequence if exists " + sequenceName;
	}

}
