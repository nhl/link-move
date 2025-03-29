package com.nhl.link.move.runtime.task.sourcekeys;

import com.nhl.link.move.RowAttribute;
import com.nhl.link.move.runtime.task.common.DataSegment;
import org.dflib.DataFrame;

/**
 * @since 1.3
 */
public class SourceKeysSegment extends DataSegment<SourceKeysStage> {

    private final RowAttribute[] sourceRowsHeader;

    public SourceKeysSegment(RowAttribute[] sourceRowsHeader) {
        this.sourceRowsHeader = sourceRowsHeader;
    }

    public RowAttribute[] getSourceRowsHeader() {
        return sourceRowsHeader;
    }

    public DataFrame getSourceRows() {
        return get(SourceKeysStage.EXTRACT_SOURCE_ROWS);
    }

    /**
     * @since 3.0.0
     */
    public SourceKeysSegment setSourceRows(DataFrame df) {
        set(SourceKeysStage.EXTRACT_SOURCE_ROWS, df);
        return this;
    }

    public DataFrame getSources() {
        return get(SourceKeysStage.CONVERT_SOURCE_ROWS);
    }

    public SourceKeysSegment setSources(DataFrame df) {
        set(SourceKeysStage.CONVERT_SOURCE_ROWS, df);
        return this;
    }
}
