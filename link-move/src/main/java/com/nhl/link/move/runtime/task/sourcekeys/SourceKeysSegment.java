package com.nhl.link.move.runtime.task.sourcekeys;

import com.nhl.link.move.RowAttribute;
import com.nhl.dflib.DataFrame;
import com.nhl.link.move.runtime.task.common.DataSegment;

/**
 * @since 1.3
 */
public class SourceKeysSegment implements DataSegment {

    private final RowAttribute[] sourceRowsHeader;

    private DataFrame sourceRows;
    private DataFrame sources;

    public SourceKeysSegment(RowAttribute[] sourceRowsHeader) {
        this.sourceRowsHeader = sourceRowsHeader;
    }

    public RowAttribute[] getSourceRowsHeader() {
        return sourceRowsHeader;
    }

    public DataFrame getSourceRows() {
        return sourceRows;
    }

    /**
     * @since 3.0
     */
    public SourceKeysSegment setSourceRows(DataFrame sourceRows) {
        this.sourceRows = sourceRows;
        return this;
    }

    public DataFrame getSources() {
        return sources;
    }

    public SourceKeysSegment setSources(DataFrame sources) {
        this.sources = sources;
        return this;
    }
}
