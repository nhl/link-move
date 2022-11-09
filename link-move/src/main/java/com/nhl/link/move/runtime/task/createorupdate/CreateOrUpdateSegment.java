package com.nhl.link.move.runtime.task.createorupdate;

import com.nhl.dflib.DataFrame;
import com.nhl.link.move.RowAttribute;
import com.nhl.link.move.runtime.task.common.DataSegment;
import org.apache.cayenne.ObjectContext;

/**
 * @since 1.3
 */
public class CreateOrUpdateSegment implements DataSegment {

    public static final String KEY_COLUMN = "$lm_key";
    public static final String TARGET_COLUMN = "$lm_target";
    public static final String TARGET_CREATED_COLUMN = "$lm_target_created";

    private final ObjectContext context;
    private final RowAttribute[] sourceRowsHeader;

    private DataFrame sourceRows;
    private DataFrame sources;
    private DataFrame mappedSources;
    private DataFrame matchedTargets;
    private DataFrame mapped;
    private DataFrame merged;
    private DataFrame fksResolved;

    public CreateOrUpdateSegment(ObjectContext context, RowAttribute[] sourceRowsHeader) {
        this.context = context;
        this.sourceRowsHeader = sourceRowsHeader;
    }

    public ObjectContext getContext() {
        return context;
    }

    public DataFrame getSourceRows() {
        return sourceRows;
    }

    /**
     * @since 2.17
     */
    public CreateOrUpdateSegment setSourceRows(DataFrame sourceRows) {
        this.sourceRows = sourceRows;
        return this;
    }

    public RowAttribute[] getSourceRowsHeader() {
        return sourceRowsHeader;
    }

    public DataFrame getSources() {
        return sources;
    }

    public CreateOrUpdateSegment setSources(DataFrame sources) {
        this.sources = sources;
        return this;
    }

    public DataFrame getMappedSources() {
        return mappedSources;
    }

    public CreateOrUpdateSegment setMappedSources(DataFrame mappedSegment) {
        this.mappedSources = mappedSegment;
        return this;
    }

    public DataFrame getMatchedTargets() {
        return matchedTargets;
    }

    public CreateOrUpdateSegment setMatchedTargets(DataFrame matchedTargets) {
        this.matchedTargets = matchedTargets;
        return this;
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
    public CreateOrUpdateSegment setMapped(DataFrame mapped) {
        this.mapped = mapped;
        return this;
    }

    public DataFrame getMerged() {
        return merged;
    }

    public CreateOrUpdateSegment setMerged(DataFrame merged) {
        this.merged = merged;
        return this;
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
    public CreateOrUpdateSegment setFksResolved(DataFrame fksResolved) {
        this.fksResolved = fksResolved;
        return this;
    }
}
