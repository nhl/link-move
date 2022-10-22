package com.nhl.link.move.writer;

/**
 * @since 1.6
 */
public interface ITargetPropertyWriterService {

    TargetPropertyWriterFactory getWriterFactory(Class<?> type);
}
