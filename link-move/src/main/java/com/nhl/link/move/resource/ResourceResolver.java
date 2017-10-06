package com.nhl.link.move.resource;

import java.io.Reader;

/**
 * @since 2.4
 */
public interface ResourceResolver {

    Reader reader(String location);

    default boolean needsReload(String location, long lastLoadedOn) {
        return false;
    }
}
