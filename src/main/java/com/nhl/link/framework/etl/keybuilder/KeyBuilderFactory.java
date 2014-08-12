package com.nhl.link.framework.etl.keybuilder;

import java.util.HashMap;
import java.util.Map;

public class KeyBuilderFactory implements IKeyBuilderFactory {

	private KeyBuilder noopBuilder;
	private Map<Class<?>, KeyBuilder> builders;

	public KeyBuilderFactory() {
		this.noopBuilder = new KeyBuilder() {

			@Override
			public Object toKey(Object rawKey) {
				return rawKey;
			}
		};

		this.builders = new HashMap<>();
		this.builders.put(byte[].class, new ByteArrayKeyBuilder());
	}

	@Override
	public KeyBuilder keyBuilder(Class<?> type) {
		KeyBuilder b = builders.get(type);

		return b != null ? b : noopBuilder;
	}

}
