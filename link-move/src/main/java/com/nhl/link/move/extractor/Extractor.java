package com.nhl.link.move.extractor;

import com.nhl.link.move.Execution;
import com.nhl.link.move.RowReader;

/**
 * A unified abstraction that allows to stream data row-by-row from a variety of data sources.
 */
public interface Extractor {

    /**
     * @since 3.0.0
     */
    RowReader getReader(Execution exec);
}
