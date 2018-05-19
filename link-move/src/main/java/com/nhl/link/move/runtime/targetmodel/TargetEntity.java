package com.nhl.link.move.runtime.targetmodel;

/**
 * A collection of {@link TargetAttribute TargetAttributes}.
 *
 * @since 2.6
 */
public interface TargetEntity {

    /**
     * Returns attribute matching the path. Path can be different forms - object, db:.
     *
     * @param path a Cayenne path. Can be "db:" or "obj:" or implicit.
     * @return a target attribute or null if the path is invalid.
     */
    TargetAttribute getAttribute(String path);
}
