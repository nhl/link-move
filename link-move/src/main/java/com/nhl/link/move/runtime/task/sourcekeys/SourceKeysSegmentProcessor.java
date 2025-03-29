package com.nhl.link.move.runtime.task.sourcekeys;

import com.nhl.link.move.Execution;
import com.nhl.link.move.runtime.task.common.CallbackExecutor;
import com.nhl.link.move.runtime.task.createorupdate.RowConverter;

import java.util.Set;

/**
 * @since 1.3
 */
public class SourceKeysSegmentProcessor {

    private final CallbackExecutor<SourceKeysStage, SourceKeysSegment> callbackExecutor;
    private final RowConverter rowConverter;
    private final SourceKeysCollector mapper;

    public SourceKeysSegmentProcessor(
            RowConverter rowConverter,
            SourceKeysCollector mapper,
            CallbackExecutor<SourceKeysStage, SourceKeysSegment> callbackExecutor) {
        this.rowConverter = rowConverter;
        this.mapper = mapper;
        this.callbackExecutor = callbackExecutor;
    }

    public void process(Execution exec, SourceKeysSegment segment) {
        callbackExecutor.executeCallbacks(SourceKeysStage.EXTRACT_SOURCE_ROWS, exec, segment);

        convertSrc(exec, segment);
        collectSourceKeys(exec, segment);
    }

    private void convertSrc(Execution exec, SourceKeysSegment segment) {
        segment.setSources(rowConverter.convert(segment.getSourceRowsHeader(), segment.getSourceRows()));
        callbackExecutor.executeCallbacks(SourceKeysStage.CONVERT_SOURCE_ROWS, exec, segment);
    }

    private void collectSourceKeys(Execution exec, SourceKeysSegment segment) {

        @SuppressWarnings("unchecked")
        Set<Object> keys = (Set<Object>) exec.getAttribute(SourceKeysTask.RESULT_KEY);
        mapper.collectSourceKeys(keys, segment.getSources());

        callbackExecutor.executeCallbacks(SourceKeysStage.COLLECT_SOURCE_KEYS, exec, segment);
    }
}
