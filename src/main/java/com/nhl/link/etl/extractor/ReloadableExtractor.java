package com.nhl.link.etl.extractor;

import java.util.Map;

import com.nhl.link.etl.RowReader;
import com.nhl.link.etl.runtime.extractor.IExtractorFactory;
import com.nhl.link.etl.runtime.extractor.loader.IExtractorConfigLoader;

/**
 * An extractor decorator that can recreate the underlying {@link Extractor} if
 * the configuration object has changed underneath.
 */
public class ReloadableExtractor implements Extractor {

	private IExtractorConfigLoader configLoader;
	private Map<String, IExtractorFactory> factories;
	private String name;

	private long lastSeen;
	private Extractor delegate;

	public ReloadableExtractor(IExtractorConfigLoader configLoader, Map<String, IExtractorFactory> factories,
			String name) {
		this.configLoader = configLoader;
		this.factories = factories;
		this.name = name;
	}

	@Override
	public RowReader getReader(Map<String, ?> parameters) {
		return getDelegate().getReader(parameters);
	}

	protected Extractor getDelegate() {

		if (delegate == null || configLoader.needsReload(name, lastSeen)) {

			synchronized (this) {
				if (delegate == null || configLoader.needsReload(name, lastSeen)) {

					this.lastSeen = System.currentTimeMillis();

					ExtractorConfig config = configLoader.loadConfig(name);

					IExtractorFactory factory = factories.get(config.getType());
					if (factory == null) {
						throw new IllegalStateException("No factory mapped for Extractor type of '" + config.getType()
								+ "'");
					}

					this.delegate = factory.createExtractor(config);
				}
			}
		}

		return delegate;
	}

}
