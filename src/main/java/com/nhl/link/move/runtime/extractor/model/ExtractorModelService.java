package com.nhl.link.move.runtime.extractor.model;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.cayenne.di.Inject;

import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.extractor.model.ExtractorModel;
import com.nhl.link.move.extractor.model.ExtractorModelContainer;
import com.nhl.link.move.extractor.model.ExtractorName;

public class ExtractorModelService implements IExtractorModelService {

	private IExtractorModelLoader modelLoader;
	private ConcurrentMap<String, ExtractorModelContainer> containers;

	public ExtractorModelService(@Inject IExtractorModelLoader modelLoader) {
		this.modelLoader = modelLoader;
		this.containers = new ConcurrentHashMap<>();
	}

	@Override
	public ExtractorModel get(ExtractorName name) {

		ExtractorModelContainer c = getContainer(name.getLocation());

		ExtractorModel m = c.getExtractor(name.getName());
		if (m == null) {
			throw new LmRuntimeException("No extractor model found for name: " + name);
		}

		return m;
	}

	protected ExtractorModelContainer getContainer(String location) {

		ExtractorModelContainer c = containers.get(location);
		if (c == null || modelLoader.needsReload(c)) {
			c = modelLoader.load(location);

			// not worried about overriding a fresh container loaded by other
			// threads... In fact using 'putIfAbsent' instead of 'put' will
			// fail, as we may have an expired container stored in the map...

			containers.put(location, c);
		}

		return c;
	}

}
