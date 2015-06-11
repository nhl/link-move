package com.nhl.link.etl.runtime.extractor;

import org.apache.cayenne.di.Inject;

import com.nhl.link.etl.connect.Connector;
import com.nhl.link.etl.extractor.Extractor;
import com.nhl.link.etl.extractor.model.ExtractorModel;
import com.nhl.link.etl.runtime.connect.IConnectorService;

public abstract class BaseExtractorFactory<T extends Connector> implements IExtractorFactory {

	private IConnectorService connectorService;

	public BaseExtractorFactory(@Inject IConnectorService connectorService) {
		this.connectorService = connectorService;
	}

	@Override
	public Extractor createExtractor(ExtractorModel model) {

		T connector = connectorService.getConnector(getConnectorType(), model.getConnectorId());
		return createExtractor(connector, model);
	}

	protected abstract Class<T> getConnectorType();

	protected abstract Extractor createExtractor(T connector, ExtractorModel model);

}
