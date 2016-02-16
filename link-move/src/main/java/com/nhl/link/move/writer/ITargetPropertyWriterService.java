package com.nhl.link.move.writer;

/**
 * @since 1.6
 */
public interface ITargetPropertyWriterService {

    <T> TargetPropertyWriterFactory<T> getWriterFactory(Class<T> type);
}
