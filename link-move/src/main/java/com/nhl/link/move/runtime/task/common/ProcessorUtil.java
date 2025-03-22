package com.nhl.link.move.runtime.task.common;

import org.dflib.DataFrame;
import org.dflib.Index;

/**
 * @since 2.12
 */
public class ProcessorUtil {

    private static final String LM_COLUMN_PREFIX = "$lm_";

    public static Index dataColumns(DataFrame df) {
        return df.getColumnsIndex().selectExcept(s -> s.startsWith(LM_COLUMN_PREFIX));
    }
}
