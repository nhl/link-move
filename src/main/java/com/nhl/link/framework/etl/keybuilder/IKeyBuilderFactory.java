package com.nhl.link.framework.etl.keybuilder;

public interface IKeyBuilderFactory {

	KeyBuilder keyBuilder(Class<?> type);
}
