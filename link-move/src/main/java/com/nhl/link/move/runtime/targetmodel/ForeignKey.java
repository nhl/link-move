package com.nhl.link.move.runtime.targetmodel;

import java.util.List;

/**
 * Describes a foreign key from a source entity to a target entity.
 *
 * @since 4.0
 */
public record ForeignKey(
        List<String> sourceColumns,
        List<TargetAttribute> targets,
        String resultColumn) {

    public ForeignKey {
        if (sourceColumns.size() != targets.size()) {
            throw new IllegalArgumentException("Mismatched FK column counts: " + sourceColumns + " vs " + targets);
        }
    }

    public boolean isMultiColumn() {
        return targets.size() > 1;
    }
}
