package com.nhl.link.etl.runtime.map.key;

import com.nhl.link.etl.map.key.KeyMapAdapter;

public interface IKeyMapAdapterFactory {

	KeyMapAdapter adapter(Class<?> type);
}
