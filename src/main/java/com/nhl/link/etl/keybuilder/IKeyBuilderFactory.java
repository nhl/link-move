package com.nhl.link.etl.keybuilder;

public interface IKeyBuilderFactory {

	KeyBuilder keyBuilder(Class<?> type);
}
