package com.nhl.link.move.runtime.targetmodel;

import org.apache.cayenne.map.ObjEntity;

/**
 * A holder of {@link TargetEntity TargetEntities}.
 *
 * @since 2.6
 */
public interface TargetEntityMap {

    TargetEntity get(ObjEntity root);
}
