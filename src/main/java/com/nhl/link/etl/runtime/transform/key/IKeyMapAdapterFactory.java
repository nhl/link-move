package com.nhl.link.etl.runtime.transform.key;

public interface IKeyMapAdapterFactory {

	KeyMapAdapter adapter(Class<?> type);
}
