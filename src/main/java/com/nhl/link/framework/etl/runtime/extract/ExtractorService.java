package com.nhl.link.framework.etl.runtime.extract;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.cayenne.di.Inject;

import com.nhl.link.framework.etl.extract.Extractor;
import com.nhl.link.framework.etl.extract.ReloadableExtractor;
import com.nhl.link.framework.etl.runtime.EtlRuntimeBuilder;

public class ExtractorService implements IExtractorService {

	private IExtractorConfigLoader configLoader;
	private ConcurrentMap<String, Extractor> extractors;
	private Map<String, IExtractorFactory> factories;

	public ExtractorService(@Inject IExtractorConfigLoader configLoader,
			@Inject(EtlRuntimeBuilder.EXTRACTOR_FACTORIES_MAP) Map<String, IExtractorFactory> factories) {
		this.factories = factories;
		this.configLoader = configLoader;
		this.extractors = new ConcurrentHashMap<>();
	}

	@Override
	public Extractor getExtractor(String name) {

		Extractor extractor = extractors.get(name);

		if (extractor == null) {
			Extractor newExtractor = new ReloadableExtractor(configLoader, factories, name);
			Extractor oldExtractor = extractors.putIfAbsent(name, newExtractor);
			extractor = oldExtractor != null ? oldExtractor : newExtractor;
		}

		return extractor;
	}

}
