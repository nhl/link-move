package com.nhl.link.move.runtime.task.create;

import com.nhl.link.move.RowAttribute;
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
    private List<Object[]> sourceRows;
    private RowAttribute[] sourceRowsHeader;
    private List<Map<String, Object>> sources;
    private List<SourceTargetPair<T>> mapped;
    private List<SourceTargetPair<T>> merged;

    public CreateSegment(ObjectContext context, RowAttribute[] sourceRowsHeader, List<Object[]> rows) {
        this.sourceRowsHeader = sourceRowsHeader;
        this.sourceRows = rows;
        this.context = context;
    }

    public ObjectContext getContext() {
        return context;
    }

    public List<Object[]> getSourceRows() {
        return sourceRows;
    }

    public RowAttribute[] getSourceRowsHeader() {
        return sourceRowsHeader;
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

    public List<SourceTargetPair<T>> getMapped() {
        return mapped;
    }

    public void setMapped(List<SourceTargetPair<T>> mapped) {
        this.mapped = mapped;
    }
}
