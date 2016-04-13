package com.nhl.link.move.runtime.jdbc;

import com.nhl.link.move.LmRuntimeException;
import org.apache.cayenne.dba.TypesMapping;
import org.apache.cayenne.map.DbAttribute;

/**
 * Instances of this class are used by LM matching runtime to convert extracted sources (on per-attribute basis)
 * into a form that is comparable with target entries. Each instance of this class should handle one JDBC type.
 * By default, LM ships with the following normalizers: {@link BigIntNormalizer}, {@link IntegerNormalizer},
 * {@link DecimalNormalizer}
 */
public abstract class JdbcNormalizer {

    private final int type;
    private final String typeName;

    public JdbcNormalizer(int jdbcType) {

        String typeName = TypesMapping.getSqlNameByType(jdbcType);
        if (typeName == null) {
            throw new LmRuntimeException("Unknown jdbc type: " + jdbcType);
        }

        this.type = jdbcType;
        this.typeName = typeName;
    }

    /**
     * @return JDBC type that this normalizer works with.
     * @see java.sql.Types
     */
    public int getType() {
        return type;
    }

    public String getTypeName() {
        return typeName;
    }

    @Deprecated
    public abstract Object normalize(Object value);

    /**
     * @since 1.7
     */
    public abstract Object normalize(Object value, DbAttribute targetAttribute);
}
