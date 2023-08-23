package com.nhl.link.move.runtime.task;

import com.nhl.link.move.CreateBuilder;
import com.nhl.link.move.CreateOrUpdateBuilder;
import com.nhl.link.move.DeleteAllBuilder;
import com.nhl.link.move.DeleteBuilder;
import com.nhl.link.move.SourceKeysBuilder;

public interface ITaskService {

    /**
     * Returns a builder of "create" ETL synchronization task. Compared to {@link #createOrUpdate(Class)} builder, this
     * one is much faster, as it doesn't do key matching. It should be used for one-off data loads (such as initial
     * population of the target DB).
     *
     * @since 2.6
     * @see #create(String)
     */
    CreateBuilder create(Class<?> type);

    /**
     * Returns a builder of "create" ETL synchronization task.
     *
     * @since 3.0
     * @see #create(Class)
     */
    CreateBuilder create(String objEntityName);

    /**
     * Returns a builder of "create-or-update" ETL synchronization task.
     *
     * @since 1.3
     * @see #createOrUpdate(String)
     */
    CreateOrUpdateBuilder createOrUpdate(Class<?> type);

    /**
     * Returns a builder of "create-or-update" ETL synchronization task.
     *
     * @since 3.0
     * @see #createOrUpdate(Class)
     */
    CreateOrUpdateBuilder createOrUpdate(String objEntityName);

    /**
     * Returns a builder of target delete ETL synchronization task.
     *
     * @since 1.3
     * @see #delete(String)
     */
    DeleteBuilder delete(Class<?> type);


    /**
     * Returns a builder of target delete ETL synchronization task.
     *
     * @since 3.0
     * @see #delete(Class)
     */
    DeleteBuilder delete(String objEntityName);

    /**
     * Returns a builder of target deleteAll ETL synchronization task.
     *
     * @since 3.0
     * @see #deleteAll(String)
     */
    DeleteAllBuilder deleteAll(Class<?> type);

    /**
     * Returns a builder of target deleteAll ETL synchronization task.
     *
     * @since 3.0
     * @see #deleteAll(Class)
     */
    DeleteAllBuilder deleteAll(String objEntityName);

    /**
     * @since 1.3
     * @see #extractSourceKeys(String)
     */
    SourceKeysBuilder extractSourceKeys(Class<?> type);

    /**
     * @since 1.4
     * @see #extractSourceKeys(Class)
     */
    SourceKeysBuilder extractSourceKeys(String objEntityName);
}
