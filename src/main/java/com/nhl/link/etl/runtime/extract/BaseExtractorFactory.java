package com.nhl.link.etl.runtime.extract;

import org.apache.cayenne.di.Inject;

import com.nhl.link.etl.connect.Connector;
import com.nhl.link.etl.extract.Extractor;
import com.nhl.link.etl.extract.ExtractorConfig;
import com.nhl.link.etl.runtime.connect.IConnectorService;

public abstract class BaseExtractorFactory<T extends Connector> implements IExtractorFactory {

	private IConnectorService connectorService;

	public BaseExtractorFactory(@Inject IConnectorService connectorService) {
		this.connectorService = connectorService;
	}

	@Override
	public Extractor createExtractor(ExtractorConfig config) {

		T connector = connectorService.getConnector(getConnectorType(), config.getConnectorId());
		return createExtractor(connector, config);
	}

	protected abstract Class<T> getConnectorType();

	protected abstract Extractor createExtractor(T connector, ExtractorConfig config);

}
