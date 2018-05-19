package com.nhl.link.move.runtime.task;

import com.nhl.link.move.CreateBuilder;
import org.apache.cayenne.DataObject;

import com.nhl.link.move.CreateOrUpdateBuilder;
import com.nhl.link.move.DeleteBuilder;
import com.nhl.link.move.SourceKeysBuilder;

public interface ITaskService {

    /**
     * Returns a builder of "create" ETL synchronization task. Compared to {@link #createOrUpdate(Class)} builder, this
     * one is much faster, as it doesn't do key matching. It should be used for one-off data loads (such as initial
     * population of the target DB).
     *
     * @since 2.6
     */
    <T extends DataObject> CreateBuilder<T> create(Class<T> type);

    /**
     * Returns a builder of "create-or-update" ETL synchronization task.
     *
     * @since 1.3
     */
    <T extends DataObject> CreateOrUpdateBuilder<T> createOrUpdate(Class<T> type);

    /**
     * Returns a builder of target delete ETL synchronization task.
     *
     * @since 1.3
     */
    <T extends DataObject> DeleteBuilder<T> delete(Class<T> type);

    /**
     * @since 1.3
     */
    <T extends DataObject> SourceKeysBuilder extractSourceKeys(Class<T> type);

    /**
     * @since 1.4
     */
    SourceKeysBuilder extractSourceKeys(String targetEntityName);
}
