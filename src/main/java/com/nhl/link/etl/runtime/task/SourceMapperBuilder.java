package com.nhl.link.etl.runtime.task;

import java.util.Arrays;
import java.util.List;

import com.nhl.link.etl.mapper.AttributeMapper;
import com.nhl.link.etl.mapper.KeyAdapter;
import com.nhl.link.etl.mapper.Mapper;
import com.nhl.link.etl.mapper.MultiAttributeMapper;
import com.nhl.link.etl.mapper.SafeMapKeyMapper;
import com.nhl.link.etl.runtime.key.IKeyAdapterFactory;

public class SourceMapperBuilder {

	private IKeyAdapterFactory keyAdapterFactory;
	private List<String> columns;

	public SourceMapperBuilder(IKeyAdapterFactory keyAdapterFactory) {
		this.keyAdapterFactory = keyAdapterFactory;
	}

	public SourceMapperBuilder matchBy(String... columns) {
		this.columns = Arrays.asList(columns);
		return this;
	}

	public Mapper build() {

		if (columns == null) {
			throw new IllegalStateException("'matchBy' or 'matchById' must be set");
		}

		Mapper mapper;

		if (columns.size() > 1) {
			mapper = new MultiAttributeMapper(columns);
		} else {
			mapper = new AttributeMapper(getSingleMatchAttribute());
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
