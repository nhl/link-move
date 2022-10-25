package com.nhl.link.move.extractor;

import com.nhl.link.move.Execution;
import com.nhl.link.move.RowReader;

/**
 * Defines a service responsible that streams data from the task source. It is based on some internal query template
 * and a set of parameters passed by the caller.
 */
public interface Extractor {

    /**
     * @since 3.0
     */
    RowReader getReader(Execution exec);
}
