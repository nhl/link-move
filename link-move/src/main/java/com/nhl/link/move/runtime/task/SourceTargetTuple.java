package com.nhl.link.move.runtime.task;

import java.util.Map;

/**
 * @since 2.6
 */
public class SourceTargetTuple<T> {

    private T target;
    private Map<String, Object> source;
    private boolean created;

    public SourceTargetTuple(Map<String, Object> source, T target, boolean created) {
        this.target = target;
        this.source = source;
        this.created = created;
    }

    public T getTarget() {
        return target;
    }

    public Map<String, Object> getSource() {
        return source;
    }
    
    public boolean isCreated() {
        return created;
    }
}
