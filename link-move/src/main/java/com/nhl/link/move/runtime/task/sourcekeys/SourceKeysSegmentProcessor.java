package com.nhl.link.move.runtime.task.sourcekeys;

import com.nhl.link.move.Execution;
import com.nhl.link.move.annotation.AfterSourceRowsExtracted;
import com.nhl.link.move.runtime.task.StageListener;
import com.nhl.link.move.runtime.task.createorupdate.RowConverter;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @since 1.3
 */
public class SourceKeysSegmentProcessor {

    private final Map<Class<? extends Annotation>, List<StageListener>> listeners;
    private final RowConverter rowConverter;
    private final SourceKeysCollector mapper;

    public SourceKeysSegmentProcessor(
            RowConverter rowConverter,
            SourceKeysCollector mapper,
            Map<Class<? extends Annotation>, List<StageListener>> listeners) {
        this.rowConverter = rowConverter;
        this.mapper = mapper;
        this.listeners = listeners;
    }

    public void process(Execution exec, SourceKeysSegment segment) {
        notifyListeners(AfterSourceRowsExtracted.class, exec, segment);

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

    private void notifyListeners(Class<? extends Annotation> type, Execution exec, SourceKeysSegment segment) {
        List<StageListener> listenersOfType = listeners.get(type);
        if (listenersOfType != null) {
            for (StageListener l : listenersOfType) {
                l.afterStageFinished(exec, segment);
            }
        }
    }
}
