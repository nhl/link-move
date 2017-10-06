package com.nhl.link.move.runtime.extractor;

import com.nhl.link.move.connect.Connector;
import com.nhl.link.move.extractor.Extractor;
import com.nhl.link.move.extractor.MultiExtractor;
import com.nhl.link.move.extractor.model.ExtractorModel;
import com.nhl.link.move.extractor.model.ExtractorName;
import com.nhl.link.move.runtime.connect.IConnectorService;
import com.nhl.link.move.runtime.extractor.model.IExtractorModelService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Keeps a reference to an {@link Extractor} instance, reloading it as needed on access.
 */
class ExtractorReloader {

    private IExtractorModelService extractorModelService;
    private IConnectorService connectorService;
    private Map<String, IExtractorFactory> factories;
    private ExtractorName name;

    private long lastSeen;
    private Extractor delegate;

    public ExtractorReloader(
            IExtractorModelService extractorModelService,
            IConnectorService connectorService,
            Map<String, IExtractorFactory> factories,
            ExtractorName name) {

        this.extractorModelService = extractorModelService;
        this.connectorService = connectorService;
        this.factories = factories;
        this.name = name;
    }

    Extractor getOrReload() {
        ExtractorModel model = extractorModelService.get(name);

        if (needsReload(model)) {

            synchronized (this) {
                if (needsReload(model)) {

                    this.lastSeen = model.getLoadedOn() + 1;
                    this.delegate = createExtractor(model);
                }
            }
        }

        return delegate;
    }

    @SuppressWarnings("unchecked")
    private <T extends Connector> Extractor createExtractor(ExtractorModel model) {

        IExtractorFactory<T> factory = (IExtractorFactory<T>) factories.get(model.getType());
        if (factory == null) {
            throw new IllegalStateException("No factory mapped for Extractor type of '" + model.getType() + "'");
        }

        Collection<String> connectorIds = model.getConnectorIds();
        if (connectorIds.size() == 1) {
            return createExtractor(model, factory, connectorIds.iterator().next());
        } else {
            List<Extractor> extractors = new ArrayList<>(connectorIds.size());
            connectorIds.forEach(id -> extractors.add(createExtractor(model, factory, id)));
            return new MultiExtractor(extractors);
        }
    }

    private <T extends Connector> Extractor createExtractor(
            ExtractorModel model,
            IExtractorFactory<T> factory,
            String connectorId) {

        T connector = connectorService.getConnector(factory.getConnectorType(), connectorId);
        return factory.createExtractor(connector, model);
    }

    boolean needsReload(ExtractorModel model) {
        return delegate == null || model.getLoadedOn() > lastSeen;
    }
}
