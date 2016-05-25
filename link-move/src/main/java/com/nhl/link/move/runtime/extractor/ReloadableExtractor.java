package com.nhl.link.move.runtime.extractor;

import java.util.Map;

import com.nhl.link.move.RowReader;
import com.nhl.link.move.connect.Connector;
import com.nhl.link.move.extractor.Extractor;
import com.nhl.link.move.extractor.model.ExtractorModel;
import com.nhl.link.move.extractor.model.ExtractorName;
import com.nhl.link.move.runtime.connect.IConnectorService;
import com.nhl.link.move.runtime.extractor.model.IExtractorModelService;

/**
 * An extractor decorator that can recreate the underlying {@link Extractor} if
 * the configuration object has changed underneath.
 */
public class ReloadableExtractor implements Extractor {

	private IExtractorModelService extractorModelService;
	private IConnectorService connectorService;
	private Map<String, IExtractorFactory<? extends Connector>> factories;
	private ExtractorName name;

	private long lastSeen;
	private Extractor delegate;

	public ReloadableExtractor(IExtractorModelService extractorModelService, IConnectorService connectorService,
			Map<String, IExtractorFactory<? extends Connector>> factories, ExtractorName name) {

		this.extractorModelService = extractorModelService;
		this.connectorService = connectorService;
		this.factories = factories;
		this.name = name;
	}

	@Override
	public RowReader getReader(Map<String, ?> parameters) {
		return getDelegate().getReader(parameters);
	}

	protected Extractor getDelegate() {

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

		T connector = connectorService.getConnector(factory.getConnectorType(), model.getConnectorId());
		return factory.createExtractor(connector, model);
	}

	boolean needsReload(ExtractorModel model) {
		return delegate == null || model.getLoadedOn() > lastSeen;
	}
}
