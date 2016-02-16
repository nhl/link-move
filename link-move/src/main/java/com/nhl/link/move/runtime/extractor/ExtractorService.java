package com.nhl.link.move.runtime.extractor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.nhl.link.move.connect.Connector;
import com.nhl.link.move.runtime.connect.IConnectorService;
import org.apache.cayenne.di.Inject;

import com.nhl.link.move.extractor.Extractor;
import com.nhl.link.move.extractor.model.ExtractorName;
import com.nhl.link.move.runtime.LmRuntimeBuilder;
import com.nhl.link.move.runtime.extractor.model.IExtractorModelService;

public class ExtractorService implements IExtractorService {

	private IExtractorModelService modelService;
	private IConnectorService connectorService;
	private ConcurrentMap<ExtractorName, Extractor> extractors;
	private Map<String, IExtractorFactory<? extends Connector>> factories;

	public ExtractorService(@Inject IExtractorModelService modelService, @Inject IConnectorService connectorService,
			@Inject(LmRuntimeBuilder.EXTRACTOR_FACTORIES_MAP) Map<String, IExtractorFactory<? extends Connector>> factories) {
		this.factories = factories;
		this.modelService = modelService;
		this.connectorService = connectorService;
		this.extractors = new ConcurrentHashMap<>();
	}

	@Override
	public Extractor getExtractor(ExtractorName name) {

		Extractor extractor = extractors.get(name);

		if (extractor == null) {
			Extractor newExtractor = new ReloadableExtractor(modelService, connectorService, factories, name);
			Extractor oldExtractor = extractors.putIfAbsent(name, newExtractor);
			extractor = oldExtractor != null ? oldExtractor : newExtractor;
		}

		return extractor;
	}

}
