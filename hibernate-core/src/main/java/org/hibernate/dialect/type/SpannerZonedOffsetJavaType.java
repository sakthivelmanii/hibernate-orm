/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.dialect.type;

import org.hibernate.type.descriptor.WrapperOptions;
import org.hibernate.type.descriptor.java.ZoneOffsetJavaType;

import java.time.ZoneOffset;

public class SpannerZonedOffsetJavaType extends ZoneOffsetJavaType {

	public static final SpannerZonedOffsetJavaType INSTANCE = new SpannerZonedOffsetJavaType();

	@Override
	public <X> X unwrap(ZoneOffset value, Class<X> type, WrapperOptions wrapperOptions) {
		if ( value == null ) {
			return null;
		}
		if ( Long.class.isAssignableFrom( type ) ) {
			return type.cast( (long) value.getTotalSeconds() );
		}
		return super.unwrap( value, type, wrapperOptions );
	}

	@Override
	public <X> ZoneOffset wrap(X value, WrapperOptions wrapperOptions) {
		if ( value == null ) {
			return null;
		}
		if ( value instanceof Long longValue && isIntegerValue(longValue)) {
			return ZoneOffset.ofTotalSeconds( longValue.intValue() );
		}

		return super.wrap( value, wrapperOptions );
	}

	private boolean isIntegerValue(Long longValue) {
		return longValue >= Integer.MIN_VALUE && longValue <= Integer.MAX_VALUE;
	}
}
