package com.nhl.link.move.runtime.extractor;

import com.nhl.link.move.extractor.Extractor;
import com.nhl.link.move.extractor.model.ExtractorName;
import com.nhl.link.move.runtime.connect.IConnectorService;
import com.nhl.link.move.runtime.extractor.model.IExtractorModelService;
import org.apache.cayenne.di.Inject;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ExtractorService implements IExtractorService {

    private final IExtractorModelService modelService;
    private final IConnectorService connectorService;
    private final Map<String, IExtractorFactory> factories;
    private final Map<ExtractorName, ExtractorReloader> extractorCache;

    public ExtractorService(
            @Inject IExtractorModelService modelService,
            @Inject IConnectorService connectorService,
            @Inject Map<String, IExtractorFactory> factories) {

        this.factories = factories;
        this.modelService = modelService;
        this.connectorService = connectorService;
        this.extractorCache = new ConcurrentHashMap<>();
    }

    @Override
    public Extractor getExtractor(ExtractorName name) {
        return extractorCache.computeIfAbsent(name, n
                        -> new ExtractorReloader(modelService, connectorService, factories, n))
                .getOrReload();
    }
}
