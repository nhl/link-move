package com.nhl.link.etl.runtime.matcher;

import java.util.HashMap;
import java.util.Map;

import com.nhl.link.etl.load.matcher.ByteArrayKeyAdapter;
import com.nhl.link.etl.load.matcher.KeyAdapter;

public class KeyAdapterFactory implements IKeyAdapterFactory {

	private KeyAdapter noopBuilder;
	private Map<Class<?>, KeyAdapter> builders;

	public KeyAdapterFactory() {
		this.noopBuilder = new KeyAdapter() {

			@Override
			public Object toMapKey(Object rawKey) {
				return rawKey;
			}
			
			@Override
			public Object fromMapKey(Object mapKey) {
				return mapKey;
			}
		};

		this.builders = new HashMap<>();
		this.builders.put(byte[].class, new ByteArrayKeyAdapter());
	}

	@Override
	public KeyAdapter adapter(Class<?> type) {
		KeyAdapter b = builders.get(type);

		return b != null ? b : noopBuilder;
	}

}
