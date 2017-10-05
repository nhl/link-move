package com.nhl.link.move.runtime.extractor.model;

import com.nhl.link.move.extractor.model.ExtractorModel;
import com.nhl.link.move.extractor.model.ExtractorName;

/**
 * A service that provides access to extractor models regardless of their
 * location.
 *
 * @since 1.4
 */
public interface IExtractorModelService {

    /**
     * Returns an {@link ExtractorModel} matching the name.
     *
     * @param name a fully-qualified name of the extractor within container.
     */
    ExtractorModel get(ExtractorName name);
}
