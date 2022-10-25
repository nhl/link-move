package com.nhl.link.move.runtime.extractor;

import com.nhl.link.move.extractor.Extractor;
import com.nhl.link.move.extractor.model.ExtractorName;

public interface IExtractorService {

    /**
     * Returns an {@link Extractor} identified by {@link ExtractorName} or throws an exception if such an extractor
     * is not available.
     */
    Extractor getExtractor(ExtractorName name);
}
