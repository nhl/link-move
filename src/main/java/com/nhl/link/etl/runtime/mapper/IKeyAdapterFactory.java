package com.nhl.link.etl.runtime.mapper;

import com.nhl.link.etl.mapper.KeyAdapter;

public interface IKeyAdapterFactory {

	KeyAdapter adapter(Class<?> type);
}
