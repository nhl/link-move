package com.nhl.link.move.runtime.task.create;

import com.nhl.link.move.Row;
import com.nhl.link.move.runtime.task.SourceTargetPair;
import org.apache.cayenne.ObjectContext;

import java.util.List;
import java.util.Map;

/**
 * @param <T>
 * @since 2.6
 */
public class CreateSegment<T> {

    private ObjectContext context;
    private List<Row> sourceRows;
    private List<Map<String, Object>> sources;
    private List<SourceTargetPair<T>> merged;

    public CreateSegment(ObjectContext context, List<Row> rows) {
        this.sourceRows = rows;
        this.context = context;
    }

    public ObjectContext getContext() {
        return context;
    }

    public List<Row> getSourceRows() {
        return sourceRows;
    }

    public List<Map<String, Object>> getSources() {
        return sources;
    }

    public void setSources(List<Map<String, Object>> translatedSegment) {
        this.sources = translatedSegment;
    }

    public List<SourceTargetPair<T>> getMerged() {
        return merged;
    }

    public void setMerged(List<SourceTargetPair<T>> merged) {
        this.merged = merged;
    }
}
