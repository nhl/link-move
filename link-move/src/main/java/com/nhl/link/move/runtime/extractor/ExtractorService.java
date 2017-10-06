package com.nhl.link.move.runtime.extractor;

import com.nhl.link.move.extractor.Extractor;
import com.nhl.link.move.extractor.model.ExtractorName;
import com.nhl.link.move.runtime.connect.IConnectorService;
import com.nhl.link.move.runtime.extractor.model.IExtractorModelService;
import org.apache.cayenne.di.Inject;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ExtractorService implements IExtractorService {

    private IExtractorModelService modelService;
    private IConnectorService connectorService;
    private ConcurrentMap<ExtractorName, Extractor> extractors;
    private Map<String, IExtractorFactory> factories;

    public ExtractorService(@Inject IExtractorModelService modelService,
                            @Inject IConnectorService connectorService,
                            @Inject Map<String, IExtractorFactory> factories) {
        this.factories = factories;
        this.modelService = modelService;
        this.connectorService = connectorService;
        this.extractors = new ConcurrentHashMap<>();
    }

    @Override
    public Extractor getExtractor(ExtractorName name) {

        Extractor extractor = extractors.get(name);

        if (extractor == null) {
            ExtractorReloader reloader = new ExtractorReloader(modelService, connectorService, factories, name);
            Extractor newExtractor = params -> reloader.getOrReload().getReader(params);
            Extractor oldExtractor = extractors.putIfAbsent(name, newExtractor);
            extractor = oldExtractor != null ? oldExtractor : newExtractor;
        }

        return extractor;
    }

}
