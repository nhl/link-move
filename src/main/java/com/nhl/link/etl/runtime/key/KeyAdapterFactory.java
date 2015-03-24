package com.nhl.link.etl.runtime.key;

import java.util.HashMap;
import java.util.Map;

import com.nhl.link.etl.mapper.ByteArrayKeyAdapter;
import com.nhl.link.etl.mapper.KeyAdapter;

public class KeyAdapterFactory implements IKeyAdapterFactory {

	private KeyAdapter noopAdapter;
	private Map<Class<?>, KeyAdapter> adapters;

	public KeyAdapterFactory() {
		this.noopAdapter = new KeyAdapter() {

			@Override
			public Object toMapKey(Object rawKey) {
				return rawKey;
			}
			
			@Override
			public Object fromMapKey(Object mapKey) {
				return mapKey;
			}
		};

		this.adapters = new HashMap<>();
		this.adapters.put(byte[].class, new ByteArrayKeyAdapter());
	}

	@Override
	public KeyAdapter adapter(Class<?> type) {
		KeyAdapter b = adapters.get(type);

		return b != null ? b : noopAdapter;
	}

}
