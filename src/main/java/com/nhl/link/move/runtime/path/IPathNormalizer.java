package com.nhl.link.move.runtime.path;

import org.apache.cayenne.map.ObjEntity;

/**
 * @since 1.4
 */
public interface IPathNormalizer {

	EntityPathNormalizer normalizer(ObjEntity root);
}
