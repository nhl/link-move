package com.nhl.link.etl.runtime.load.mapper;

import com.nhl.link.etl.load.mapper.KeyAdapter;

public interface IKeyAdapterFactory {

	KeyAdapter adapter(Class<?> type);
}
