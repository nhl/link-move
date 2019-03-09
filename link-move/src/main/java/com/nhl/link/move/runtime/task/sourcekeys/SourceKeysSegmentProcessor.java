package com.nhl.link.move.runtime.task.sourcekeys;

import com.nhl.link.move.Execution;
import com.nhl.link.move.runtime.task.createorupdate.RowConverter;

import java.util.Set;

/**
 * @since 1.3
 */
public class SourceKeysSegmentProcessor {

    private RowConverter rowConverter;
    private SourceKeysCollector mapper;

    public SourceKeysSegmentProcessor(RowConverter rowConverter, SourceKeysCollector mapper) {
        this.rowConverter = rowConverter;
        this.mapper = mapper;
    }

    public void process(Execution exec, SourceKeysSegment segment) {
        convertSrc(exec, segment);
        collectSourceKeys(exec, segment);
    }

    private void convertSrc(Execution exec, SourceKeysSegment segment) {
        segment.setSources(rowConverter.convert(segment.getSourceRowsHeader(), segment.getSourceRows()));
    }

    private void collectSourceKeys(Execution exec, SourceKeysSegment segment) {

        @SuppressWarnings("unchecked")
        Set<Object> keys = (Set<Object>) exec.getAttribute(SourceKeysTask.RESULT_KEY);
        mapper.collectSourceKeys(keys, segment.getSources());
    }
}
