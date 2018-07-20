package com.nhl.link.move.runtime.targetmodel;

import java.util.Optional;

/**
 * A collection of {@link TargetAttribute TargetAttributes}.
 *
 * @since 2.6
 */
public interface TargetEntity {

    String getName();

    /**
     * Returns an attribute matching the path, wrapped in an optional. Path can be in different forms compatible with
     * Cayenne path expressions ("db:", "obj:" or implicit object path).
     *
     * @param path a Cayenne path. Can be "db:" or "obj:" or implicit.
     * @return a target attribute wrapped in an optional. Empty optional is returned for paths that are not valid
     * Cayenne paths.
     */
    Optional<TargetAttribute> getAttribute(String path);
}
