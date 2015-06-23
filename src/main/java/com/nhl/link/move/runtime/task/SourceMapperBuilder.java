package com.nhl.link.move.runtime.task;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.nhl.link.move.mapper.KeyAdapter;
import com.nhl.link.move.mapper.Mapper;
import com.nhl.link.move.mapper.MultiPathMapper;
import com.nhl.link.move.mapper.PathMapper;
import com.nhl.link.move.mapper.SafeMapKeyMapper;
import com.nhl.link.move.runtime.key.IKeyAdapterFactory;
import com.nhl.link.move.runtime.path.EntityPathNormalizer;

public class SourceMapperBuilder {

	private IKeyAdapterFactory keyAdapterFactory;
	private List<String> paths;
	private Mapper mapper;
	private EntityPathNormalizer pathNormalizer;

	public SourceMapperBuilder(EntityPathNormalizer pathNormalizer, IKeyAdapterFactory keyAdapterFactory) {
		this.keyAdapterFactory = keyAdapterFactory;
		this.pathNormalizer = pathNormalizer;
	}

	public SourceMapperBuilder matchBy(Mapper mapper) {
		this.mapper = mapper;
		this.paths = null;
		return this;
	}

	public SourceMapperBuilder matchBy(String... paths) {
		this.mapper = null;

		this.paths = new ArrayList<>(paths.length);
		for (String p : paths) {
			this.paths.add(pathNormalizer.normalize(p));
		}

		return this;
	}

	public Mapper build() {

		// not wrapping custom matcher, presuming the user knows what's he's
		// doing and his matcher generates proper keys
		if (this.mapper != null) {
			return this.mapper;
		}

		if (paths == null) {
			throw new IllegalStateException("'matchBy' or 'matchById' must be set");
		}

		Mapper mapper;

		if (paths.size() > 1) {

			// ensuring predictable attribute iteration order with
			// LinkedHashMap. Useful for unit test for one thing.
			Map<String, Mapper> attributeMappers = new LinkedHashMap<>();
			for (String a : paths) {
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
		if (paths.size() > 1) {
			keyAdapter = keyAdapterFactory.adapter(List.class);
		} else {
			// TODO: since we don't have metadata upfront, create KeyAdapter
			// dynamically based on incoming data type
			keyAdapter = keyAdapterFactory.adapter(Object.class);
		}

		return new SafeMapKeyMapper(mapper, keyAdapter);
	}

	private String getSingleMatchAttribute() {
		if (paths == null || paths.isEmpty()) {
			return null;
		}

		if (paths.size() > 1) {
			throw new IllegalStateException("Trying to get a single match attribute but multi key matching is set");
		}

		return paths.get(0);
	}

}
