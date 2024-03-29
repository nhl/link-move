package com.nhl.link.move.writer;

import com.nhl.link.move.LmRuntimeException;
import org.apache.cayenne.Persistent;
import org.apache.cayenne.map.DbAttribute;
import org.apache.cayenne.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @since 1.4
 */
public class TargetPkPropertyWriter implements TargetPropertyWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(TargetPkPropertyWriter.class);

    private final DbAttribute pk;
    private boolean complainAboutAutoPk;

    public TargetPkPropertyWriter(DbAttribute pk) {

        if (!pk.isPrimaryKey()) {
            throw new LmRuntimeException("'" + pk.getName() + "' is not a PK");
        }

        // record the need to complain about syncing an auto-pk, but do the complaining later when the situation
        // actually arises
        if (pk.isGenerated()) {
            this.complainAboutAutoPk = true;
        }

        this.pk = pk;
    }

    @Override
    public void write(Object target, Object value) {

        Persistent pt = (Persistent) target;

        // regular meaningless PK
        pt.getObjectId().getReplacementIdMap().put(pk.getName(), value);
    }

    @Override
    public boolean willWrite(Object target, Object value) {

        Persistent pt = (Persistent) target;

        Map<String, Object> idSnapshot = pt.getObjectId().getIdSnapshot();
        if (Util.nullSafeEquals(idSnapshot.get(pk.getName()), value)) {
            return false;
        }

        Map<String, Object> replacementMap = pt.getObjectId().getReplacementIdMap();
        if (Util.nullSafeEquals(replacementMap.get(pk.getName()), value)) {
            return false;
        }

        if (complainAboutAutoPk) {

            // Complain, but only once

            // Not too worried about race conditions while resetting "complainAboutAutoPk". if we complain a few extra
            // times, it is no big deal

            complainAboutAutoPk = false;
            LOGGER.warn("ID '{}' is autogenerated. It is not advised to synchronize auto-generated columns from source", pk.getName());
        }

        return true;
    }
}
