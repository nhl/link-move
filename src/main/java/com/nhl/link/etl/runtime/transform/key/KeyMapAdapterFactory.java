package com.nhl.link.etl.runtime.transform.key;

import java.util.HashMap;
import java.util.Map;

public class KeyMapAdapterFactory implements IKeyMapAdapterFactory {

	private KeyMapAdapter noopBuilder;
	private Map<Class<?>, KeyMapAdapter> builders;

	public KeyMapAdapterFactory() {
		this.noopBuilder = new KeyMapAdapter() {

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
		this.builders.put(byte[].class, new ByteArrayKeyMapAdapter());
	}

	@Override
	public KeyMapAdapter adapter(Class<?> type) {
		KeyMapAdapter b = builders.get(type);

		return b != null ? b : noopBuilder;
	}

}
