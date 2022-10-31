package com.nhl.link.move;

import org.apache.cayenne.exp.Expression;

/**
 * since 3.0
 */
public interface DeleteAllBuilder {

    /**
     * Creates a new task that deletes target objects that match the target filter if filter exists.
     * If no target filter provided then it deletes all rows entirely:
     * - using truncate, if database supports truncate queries.
     * - if database doesn't support truncate queries then it deletes with delete all query
     */
    LmTask task() throws IllegalStateException;

    DeleteAllBuilder targetFilter(Expression filter);

    /**
     * For large tables, it may improve performance of deleteAll operation without targetFilter in databases that supports truncate queries.
     * Truncate query does not return count of deleted rows, so additional select count is performed by default.
     * This flag disables additional select, so execution statistics will be empty.
     * If targetFilter specified of database doesn't support truncate then this flag going to be ignored.
     */
    DeleteAllBuilder skipExecutionStats();
}
