/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.vector.internal;

import java.util.Locale;

import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.Size;
import org.hibernate.metamodel.mapping.SqlExpressible;
import org.hibernate.type.Type;
import org.hibernate.type.descriptor.sql.spi.DdlTypeRegistry;

/**
 * Spanner GoogleSQL DDL type for vector types with support for dimension constraints
 * and quantization formats (SFP8, SFP4).
 *
 * @since 7.2
 */
public class SpannerVectorDdlType extends VectorDdlType {

	private final String baseType;
	private final String quantizationFormat;

	public SpannerVectorDdlType(int sqlTypeCode, String baseType, Dialect dialect) {
		this( sqlTypeCode, baseType, null, dialect );
	}

	public SpannerVectorDdlType(int sqlTypeCode, String baseType, String quantizationFormat, Dialect dialect) {
		super( sqlTypeCode, "ARRAY<" + baseType + ">", "ARRAY<" + baseType + ">", dialect );
		this.baseType = baseType;
		if ( quantizationFormat != null && !quantizationFormat.isBlank() ) {
			if ( !"FLOAT32".equalsIgnoreCase( baseType ) ) {
				throw new IllegalArgumentException(
						"Quantization format is only supported on ARRAY<FLOAT32>, not ARRAY<" + baseType + ">"
				);
			}
			final String normalized = normalizeQuantizationFormat( quantizationFormat );
			if ( !"SFP8".equals( normalized ) && !"SFP4".equals( normalized ) ) {
				throw new IllegalArgumentException(
						"Unsupported quantization format: " + quantizationFormat + ". Supported formats are 'SFP8' and 'SFP4'."
				);
			}
			this.quantizationFormat = normalized;
		}
		else {
			this.quantizationFormat = null;
		}
	}

	public String getBaseType() {
		return baseType;
	}

	public String getQuantizationFormat() {
		return quantizationFormat;
	}

	@Override
	public String getTypeName(Size size, Type type, DdlTypeRegistry ddlTypeRegistry) {
		final String effectiveFormat = resolveQuantizationFormat( size );
		return renderTypeName( size, effectiveFormat );
	}

	public String getTypeName(Size size) {
		return getTypeName( size, (Type) null, null );
	}

	public String getTypeName(Size size, String quantizationFormat) {
		final String formatToUse;
		if ( quantizationFormat != null && !quantizationFormat.isBlank() ) {
			if ( !"FLOAT32".equalsIgnoreCase( baseType ) ) {
				throw new IllegalArgumentException(
						"Quantization format is only supported on ARRAY<FLOAT32>, not ARRAY<" + baseType + ">"
				);
			}
			final String normalized = normalizeQuantizationFormat( quantizationFormat );
			if ( !"SFP8".equals( normalized ) && !"SFP4".equals( normalized ) ) {
				throw new IllegalArgumentException(
						"Unsupported quantization format: " + quantizationFormat + ". Supported formats are 'SFP8' and 'SFP4'."
				);
			}
			formatToUse = normalized;
		}
		else {
			formatToUse = null;
		}
		return renderTypeName( size, formatToUse );
	}

	@Override
	public String getCastTypeName(Size size, SqlExpressible type, DdlTypeRegistry ddlTypeRegistry) {
		return "ARRAY<" + baseType + ">";
	}

	private String resolveQuantizationFormat(Size size) {
		if ( !"FLOAT32".equalsIgnoreCase( baseType ) ) {
			return null;
		}
		if ( size != null ) {
			final Integer precision = size.getPrecision();
			if ( precision != null ) {
				if ( precision == 8 ) {
					return "SFP8";
				}
				if ( precision == 4 ) {
					return "SFP4";
				}
			}
			final Integer scale = size.getScale();
			if ( scale != null ) {
				if ( scale == 8 ) {
					return "SFP8";
				}
				if ( scale == 4 ) {
					return "SFP4";
				}
			}
		}
		return this.quantizationFormat;
	}

	private String renderTypeName(Size size, String format) {
		final Integer arrayLength = size != null ? size.getArrayLength() : null;
		final boolean hasLength = arrayLength != null && arrayLength > 0;
		final boolean hasQuantization = format != null;

		if ( hasLength || hasQuantization ) {
			final StringBuilder sb = new StringBuilder( "ARRAY<" ).append( baseType ).append( ">(" );
			if ( hasLength ) {
				sb.append( "vector_length=>" ).append( arrayLength );
			}
			if ( hasQuantization ) {
				if ( hasLength ) {
					sb.append( ", " );
				}
				sb.append( "quantization_format=>'" ).append( format ).append( "'" );
			}
			sb.append( ")" );
			return sb.toString();
		}
		return "ARRAY<" + baseType + ">";
	}

	private static String normalizeQuantizationFormat(String format) {
		if ( format == null || format.isBlank() ) {
			return null;
		}
		String clean = format.trim();
		if ( ( clean.startsWith( "'" ) && clean.endsWith( "'" ) )
				|| ( clean.startsWith( "\"" ) && clean.endsWith( "\"" ) ) ) {
			clean = clean.substring( 1, clean.length() - 1 ).trim();
		}
		return clean.toUpperCase( Locale.ROOT );
	}
}
