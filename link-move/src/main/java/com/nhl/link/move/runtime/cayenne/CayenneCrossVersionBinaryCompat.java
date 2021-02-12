package com.nhl.link.move.runtime.cayenne;

import org.apache.cayenne.ObjectId;
import org.apache.cayenne.Persistent;
import org.apache.cayenne.map.DbAttribute;
import org.apache.cayenne.map.DbEntity;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Map;
import java.util.stream.Stream;

/**
 * A compatibility wrapper for using Cayenne across versions.
 *
 * @since 2.14
 */
public class CayenneCrossVersionBinaryCompat {

    private static final MethodHandle getIdSnapshot;
    private static final MethodHandle getReplacementIdMap;

    static {
        MethodHandles.Lookup lookup = MethodHandles.publicLookup();
        MethodType mt = MethodType.methodType(Map.class);

        try {
            getIdSnapshot = lookup.findVirtual(ObjectId.class, "getIdSnapshot", mt);
            getReplacementIdMap = lookup.findVirtual(ObjectId.class, "getReplacementIdMap", mt);
        } catch (Exception e) {
            throw new RuntimeException("Error compiling ObjectId method handles");
        }
    }

    /**
     * An alternative to {@link DbEntity#getPrimaryKeys()} that allows us to keep LM's binary compatibility between
     * Cayenne 4.0, 4.1 and 4.2. {@link DbEntity#getPrimaryKeys()} was changed to return List instead of Collection in
     * 4.2.
     */
    public static Stream<DbAttribute> pkAttributes(DbEntity entity) {
        return entity.getAttributes().stream().filter(DbAttribute::isPrimaryKey);
    }

    public static Map<String, Object> getIdSnapshot(Persistent o) {
        // this prevents the following error:
        // "java.lang.IncompatibleClassChangeError: Found interface org.apache.cayenne.ObjectId, but class was expected"

        try {
            return (Map<String, Object>) getIdSnapshot.invoke(o.getObjectId());
        } catch (Throwable th) {
            throw new RuntimeException("Unexpected...", th);
        }
    }

    public static Map<String, Object> getReplacementIdMap(Persistent o) {
        // this prevents the following error:
        // "java.lang.IncompatibleClassChangeError: Found interface org.apache.cayenne.ObjectId, but class was expected"

        try {
            return (Map<String, Object>) getReplacementIdMap.invoke(o.getObjectId());
        } catch (Throwable th) {
            throw new RuntimeException("Unexpected...", th);
        }
    }
}
