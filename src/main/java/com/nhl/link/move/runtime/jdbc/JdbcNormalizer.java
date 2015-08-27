package com.nhl.link.move.runtime.jdbc;

import com.nhl.link.move.LmRuntimeException;
import org.apache.cayenne.dba.TypesMapping;

/**
 * Instances of this class are used by LM matching runtime to convert extracted sources (on per-attribute basis)
 * into a form that is comparable with target entries. Each instance of this class should handle one JDBC type.
 * By default, LM ships with following normalizers: {@link com.nhl.link.move.runtime.jdbc.BigIntNormalizer}
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

    public abstract Object normalize(Object value);
}
