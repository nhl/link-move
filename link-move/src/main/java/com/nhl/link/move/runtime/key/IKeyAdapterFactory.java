package com.nhl.link.move.runtime.key;

import com.nhl.link.move.mapper.KeyAdapter;

public interface IKeyAdapterFactory {

	KeyAdapter adapter(Class<?> type);
}
