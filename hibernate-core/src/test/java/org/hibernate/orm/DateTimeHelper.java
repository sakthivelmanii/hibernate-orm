/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.orm;

import org.hibernate.testing.orm.junit.SessionFactoryScope;

import java.util.GregorianCalendar;
import java.util.TimeZone;

public class DateTimeHelper {
	public static GregorianCalendar get(SessionFactoryScope scope, GregorianCalendar time) {
		if (SpannerHelper.isSpannerDatabase( scope ) ) {
			time.setTimeZone( TimeZone.getTimeZone( "America/Los_Angeles" ));
		}
		return time;
	}
}
