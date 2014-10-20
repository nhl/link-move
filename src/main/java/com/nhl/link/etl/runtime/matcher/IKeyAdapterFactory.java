package com.nhl.link.etl.runtime.matcher;

import com.nhl.link.etl.load.matcher.KeyAdapter;

public interface IKeyAdapterFactory {

	KeyAdapter adapter(Class<?> type);
}
