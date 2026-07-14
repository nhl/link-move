package com.nhl.link.move.runtime.targetmodel;

import org.apache.cayenne.map.ObjEntity;

import java.util.ArrayList;
import java.util.List;

/**
 * @since 2.6
 */
public class ForeignKey {

    private final TargetEntityMap entityMap;
    private final ObjEntity targetEntity;
    private final List<String> sourceColumns;
    private final List<String> targetPaths;
    private final String resultColumn;

    private volatile List<TargetAttribute> targets;

    /**
     * @since 4.0
     */
    public ForeignKey(TargetEntityMap entityMap, ObjEntity targetEntity, List<String> sourceColumns, List<String> targetPaths, String resultColumn) {

        if (sourceColumns.size() != targetPaths.size()) {
            throw new IllegalArgumentException("Mismatched FK column counts: " + sourceColumns + " vs " + targetPaths);
        }

        this.entityMap = entityMap;
        this.targetEntity = targetEntity;
        this.sourceColumns = sourceColumns;
        this.targetPaths = targetPaths;
        this.resultColumn = resultColumn;
    }

    /**
     * @since 4.0
     */
    public boolean isMultiColumn() {
        return targetPaths.size() > 1;
    }

    /**
     * Returns the label of the DataFrame column that carries the resolved related object. It is named
     * after the relationship (e.g. "db:series"), so it is stable regardless of how many columns the
     * FK spans, and is kept separate from the raw FK source columns.
     *
     * @since 4.0
     */
    public String getResultColumn() {
        return resultColumn;
    }

    /**
     * Returns the FK column paths on the source (FK-bearing) entity in the target namespace (i.e.
     * "db:"-prefixed), aligned with {@link #getTargets()}. These match the corresponding source
     * DataFrame column labels.
     *
     * @since 4.0
     */
    public List<String> getSourceColumns() {
        return sourceColumns;
    }

    /**
     * Returns the target PK attributes this FK points at, aligned with {@link #getSourceColumns()}.
     *
     * @since 4.0
     */
    public List<TargetAttribute> getTargets() {
        if (targets == null) {
            targets = findTargets();
        }

        return targets;
    }

    private List<TargetAttribute> findTargets() {
        TargetEntity target = entityMap.get(targetEntity);
        List<TargetAttribute> result = new ArrayList<>(targetPaths.size());
        for (String targetPath : targetPaths) {
            result.add(target.getAttribute(targetPath).get());
        }
        return result;
    }
}
