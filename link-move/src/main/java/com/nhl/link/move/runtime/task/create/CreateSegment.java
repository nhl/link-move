package com.nhl.link.move.runtime.task.create;

import com.nhl.link.move.RowAttribute;
import com.nhl.dflib.DataFrame;
import org.apache.cayenne.ObjectContext;

/**
 * @since 2.6
 */
public class CreateSegment {

    public static final String TARGET_COLUMN = "$lm_target";
    public static final String TARGET_CREATED_COLUMN = "$lm_target_created";

    private final ObjectContext context;
    private final RowAttribute[] sourceRowsHeader;

    private DataFrame sourceRows;
    private DataFrame sources;
    private DataFrame mapped;
    private DataFrame fksResolved;
    private DataFrame merged;

    public CreateSegment(ObjectContext context, RowAttribute[] sourceRowsHeader, DataFrame sourceRows) {
        this.sourceRowsHeader = sourceRowsHeader;
        this.sourceRows = sourceRows;
        this.context = context;
    }

    public ObjectContext getContext() {
        return context;
    }

    public RowAttribute[] getSourceRowsHeader() {
        return sourceRowsHeader;
    }

    public DataFrame getSourceRows() {
        return sourceRows;
    }

    /**
     * @since 2.17
     */
    public void setSourceRows(DataFrame sourceRows) {
        this.sourceRows = sourceRows;
    }

    public DataFrame getSources() {
        return sources;
    }

    public void setSources(DataFrame translatedSegment) {
        this.sources = translatedSegment;
    }

    public DataFrame getMerged() {
        return merged;
    }

    public void setMerged(DataFrame merged) {
        this.merged = merged;
    }

    public DataFrame getMapped() {
        return mapped;
    }

    public void setMapped(DataFrame mapped) {
        this.mapped = mapped;
    }

    /**
     * @since 2.12
     */
    public DataFrame getFksResolved() {
        return fksResolved;
    }

    /**
     * @since 2.12
     */
    public void setFksResolved(DataFrame fksResolved) {
        this.fksResolved = fksResolved;
    }
}
