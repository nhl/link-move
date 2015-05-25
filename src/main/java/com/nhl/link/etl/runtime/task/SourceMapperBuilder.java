package com.nhl.link.etl.runtime.task;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.nhl.link.etl.mapper.PathMapper;
import com.nhl.link.etl.mapper.KeyAdapter;
import com.nhl.link.etl.mapper.Mapper;
import com.nhl.link.etl.mapper.MultiPathMapper;
import com.nhl.link.etl.mapper.SafeMapKeyMapper;
import com.nhl.link.etl.runtime.key.IKeyAdapterFactory;

public class SourceMapperBuilder {

	private IKeyAdapterFactory keyAdapterFactory;
	private List<String> columns;
	private Mapper mapper;

	public SourceMapperBuilder(IKeyAdapterFactory keyAdapterFactory) {
		this.keyAdapterFactory = keyAdapterFactory;
	}

	public SourceMapperBuilder matchBy(Mapper mapper) {
		this.mapper = mapper;
		this.columns = null;
		return this;
	}

	public SourceMapperBuilder matchBy(String... columns) {
		this.mapper = null;
		this.columns = Arrays.asList(columns);
		return this;
	}

	public Mapper build() {

		// not wrapping custom matcher, presuming the user knows what's he's
		// doing and his matcher generates proper keys
		if (this.mapper != null) {
			return this.mapper;
		}

		if (columns == null) {
			throw new IllegalStateException("'matchBy' or 'matchById' must be set");
		}

		Mapper mapper;

		if (columns.size() > 1) {
			
			// ensuring predictable attribute iteration order with
			// LinkedHashMap. Useful for unit test for one thing.
			Map<String, Mapper> attributeMappers = new LinkedHashMap<>();
			for (String a : columns) {
				attributeMappers.put(a, new PathMapper(a));
			}
			
			mapper = new MultiPathMapper(attributeMappers);
		} else {
			mapper = new PathMapper(getSingleMatchAttribute());
		}

		KeyAdapter keyAdapter;

		// TODO: mapping keyMapAdapters by type doesn't take into account
		// composition and hierarchy of the keys ... need a different approach.
		// for now resorting to the hacks below
		if (columns.size() > 1) {
			keyAdapter = keyAdapterFactory.adapter(List.class);
		} else {
			// TODO: since we don't have metadata upfront, create KeyAdapter
			// dynamically based on incoming data type
			keyAdapter = keyAdapterFactory.adapter(Object.class);
		}

		return new SafeMapKeyMapper(mapper, keyAdapter);
	}

	public String getSingleMatchAttribute() {
		if (columns == null || columns.isEmpty()) {
			return null;
		}

		if (columns.size() > 1) {
			throw new IllegalStateException("Trying to get a single match attribute but multi key matching is set");
		}
		return columns.get(0);
	}

}
