package com.nhl.link.move.runtime.task;

import java.util.Map;

/**
 * A pair of matched source row and target object.
 *
 * @since 2.6
 */
public class SourceTargetPair<T> {

    private T target;
    private Map<String, Object> source;
    private boolean created;

    public SourceTargetPair(Map<String, Object> source, T target, boolean created) {
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
