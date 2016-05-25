package com.nhl.link.move.runtime.jdbc;

import org.apache.cayenne.map.DbAttribute;

/**
 * Instances of this class are used by LM matching runtime to convert extracted
 * sources (on per-attribute basis) into a form that is comparable with target
 * entries. Each instance of this class should handle one JDBC type. By default,
 * LM ships with the following normalizers: {@link BigIntNormalizer},
 * {@link IntegerNormalizer}, {@link DecimalNormalizer},
 * {@link BooleanNormalizer}
 */
public abstract class JdbcNormalizer<T> {

	private final Class<T> type;

	public JdbcNormalizer(Class<T> javaType) {
		this.type = javaType;
	}

	/**
	 * @return JDBC type that this normalizer works with.
	 * @see java.sql.Types
	 */
	public Class<T> getType() {
		return type;
	}

	public String getTypeName() {
		return type.getName();
	}

	/**
	 * @since 1.7
	 */
	public abstract T normalize(Object value, DbAttribute targetAttribute);
}
