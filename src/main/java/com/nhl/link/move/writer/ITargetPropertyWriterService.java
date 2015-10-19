package com.nhl.link.move.writer;

public interface ITargetPropertyWriterService {

    <T> TargetPropertyWriterFactory<T> getWriterFactory(Class<T> type);
}
