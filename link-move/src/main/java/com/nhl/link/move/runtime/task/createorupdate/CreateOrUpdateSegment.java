package com.nhl.link.move.runtime.task.createorupdate;

import org.dflib.DataFrame;
import com.nhl.link.move.RowAttribute;
import com.nhl.link.move.runtime.task.common.DataSegment;
import org.apache.cayenne.ObjectContext;

/**
 * @since 1.3
 */
public class CreateOrUpdateSegment extends DataSegment<CreateOrUpdateStage> {

    public static final String KEY_COLUMN = "$lm_key";
    public static final String TARGET_COLUMN = "$lm_target";
    public static final String TARGET_CREATED_COLUMN = "$lm_target_created";

    private final ObjectContext context;
    private final RowAttribute[] sourceRowsHeader;

    public CreateOrUpdateSegment(ObjectContext context, RowAttribute[] sourceRowsHeader) {
        this.context = context;
        this.sourceRowsHeader = sourceRowsHeader;
    }

    public ObjectContext getContext() {
        return context;
    }

    public RowAttribute[] getSourceRowsHeader() {
        return sourceRowsHeader;
    }

    public DataFrame getSourceRows() {
        return get(CreateOrUpdateStage.EXTRACT_SOURCE_ROWS);
    }

    /**
     * @since 2.17
     */
    public CreateOrUpdateSegment setSourceRows(DataFrame df) {
        set(CreateOrUpdateStage.EXTRACT_SOURCE_ROWS, df);
        return this;
    }

    public DataFrame getSources() {
        return get(CreateOrUpdateStage.CONVERT_SOURCE_ROWS);
    }

    public CreateOrUpdateSegment setSources(DataFrame df) {
        set(CreateOrUpdateStage.CONVERT_SOURCE_ROWS, df);
        return this;
    }

    public DataFrame getMappedSources() {
        return get(CreateOrUpdateStage.MAP_SOURCE);
    }

    public CreateOrUpdateSegment setMappedSources(DataFrame df) {
        set(CreateOrUpdateStage.MAP_SOURCE, df);
        return this;
    }

    public DataFrame getMatchedTargets() {
        return get(CreateOrUpdateStage.MATCH_TARGET);
    }

    public CreateOrUpdateSegment setMatchedTargets(DataFrame df) {
        set(CreateOrUpdateStage.MATCH_TARGET, df);
        return this;
    }

    /**
     * @since 2.6
     */
    public DataFrame getMapped() {
        return get(CreateOrUpdateStage.MAP_TARGET);
    }

    /**
     * @since 2.6
     */
    public CreateOrUpdateSegment setMapped(DataFrame df) {
        set(CreateOrUpdateStage.MAP_TARGET, df);
        return this;
    }

    /**
     * @since 2.12
     */
    public DataFrame getFksResolved() {
        return get(CreateOrUpdateStage.RESOLVE_FK_VALUES);
    }

    /**
     * @since 2.12
     */
    public CreateOrUpdateSegment setFksResolved(DataFrame df) {
        set(CreateOrUpdateStage.RESOLVE_FK_VALUES, df);
        return this;
    }

    public DataFrame getMerged() {
        return get(CreateOrUpdateStage.MERGE_TARGET);
    }

    public CreateOrUpdateSegment setMerged(DataFrame df) {
        set(CreateOrUpdateStage.MERGE_TARGET, df);
        return this;
    }
}
