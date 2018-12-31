package com.nhl.link.move.runtime.task.createorupdate;

import com.nhl.link.move.RowAttribute;
import com.nhl.link.move.df.DataFrame;
import org.apache.cayenne.ObjectContext;

/**
 * @since 1.3
 */
public class CreateOrUpdateSegment<T> {

    public static final String KEY_COLUMN = "$lm_key";
    public static final String TARGET_COLUMN = "$lm_target";
    public static final String TARGET_CREATED_COLUMN = "$lm_target_created";
    public static final String TARGET_CHANGED_COLUMN = "$lm_target_changed";

    private ObjectContext context;
    private RowAttribute[] sourceRowsHeader;
    private DataFrame sourceRows;

    private DataFrame sources;
    private DataFrame mappedSources;
    private DataFrame matchedTargets;
    private DataFrame mapped;
    private DataFrame merged;

    public CreateOrUpdateSegment(ObjectContext context, RowAttribute[] sourceRowsHeader, DataFrame sourceRows) {
        this.sourceRows = sourceRows;
        this.context = context;
        this.sourceRowsHeader = sourceRowsHeader;
    }

    public ObjectContext getContext() {
        return context;
    }

    public DataFrame getSourceRows() {
        return sourceRows;
    }

    public RowAttribute[] getSourceRowsHeader() {
        return sourceRowsHeader;
    }

    public DataFrame getSources() {
        return sources;
    }

    public void setSources(DataFrame sources) {
        this.sources = sources;
    }

    public DataFrame getMappedSources() {
        return mappedSources;
    }

    public void setMappedSources(DataFrame mappedSegment) {
        this.mappedSources = mappedSegment;
    }

    public DataFrame getMatchedTargets() {
        return matchedTargets;
    }

    public void setMatchedTargets(DataFrame matchedTargets) {
        this.matchedTargets = matchedTargets;
    }

    /**
     * @since 2.6
     */
    public DataFrame getMapped() {
        return mapped;
    }

    /**
     * @since 2.6
     */
    public void setMapped(DataFrame mapped) {
        this.mapped = mapped;
    }

    public DataFrame getMerged() {
        return merged;
    }

    public void setMerged(DataFrame merged) {
        this.merged = merged;
    }

}
