package com.nhl.link.move.runtime.targetmodel;

import org.apache.cayenne.map.ObjEntity;

/**
 * @since 2.6
 */
public class ForeignKey {

    private TargetEntityMap entityMap;
    private ObjEntity targetEntity;
    private String targetPath;

    private volatile TargetAttribute target;

    public ForeignKey(TargetEntityMap entityMap, ObjEntity targetEntity, String targetPath) {
        this.entityMap = entityMap;
        this.targetEntity = targetEntity;
        this.targetPath = targetPath;
    }

    public TargetAttribute getTarget() {
        if (target == null) {
            target = findTarget();
        }

        return target;
    }

    private TargetAttribute findTarget() {
        return entityMap.get(targetEntity).getAttribute(targetPath).get();
    }
}
