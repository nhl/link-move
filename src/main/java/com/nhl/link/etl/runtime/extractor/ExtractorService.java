package com.nhl.link.etl.runtime.extractor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.cayenne.di.Inject;

import com.nhl.link.etl.extractor.Extractor;
import com.nhl.link.etl.extractor.ReloadableExtractor;
import com.nhl.link.etl.runtime.EtlRuntimeBuilder;
import com.nhl.link.etl.runtime.extractor.loader.IExtractorConfigLoader;

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
