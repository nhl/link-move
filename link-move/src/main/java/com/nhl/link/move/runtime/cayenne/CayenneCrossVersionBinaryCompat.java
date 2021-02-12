package com.nhl.link.move.runtime.cayenne;

import org.apache.cayenne.map.DbAttribute;
import org.apache.cayenne.map.DbEntity;

import java.util.stream.Stream;

/**
 * A compatibility wrapper for using Cayenne across versions.
 *
 * @since 2.14
 */
public class CayenneCrossVersionBinaryCompat {

    /**
     * An alternative to {@link DbEntity#getPrimaryKeys()} that allows us to keep LM's binary compatibility between
     * Cayenne 4.0, 4.1 and 4.2. {@link DbEntity#getPrimaryKeys()} was changed to return List instead of Collection in
     * 4.2.
     */
    public static Stream<DbAttribute> pkAttributes(DbEntity entity) {
        return entity.getAttributes().stream().filter(DbAttribute::isPrimaryKey);
    }
}
