package com.nhl.link.move.runtime.xml;

import com.nhl.link.move.BaseRowAttribute;
import com.nhl.link.move.connect.Connector;
import com.nhl.link.move.extractor.model.MutableExtractorModel;
import com.nhl.link.move.runtime.extractor.IExtractorFactory;
import org.junit.Before;

import static org.mockito.Mockito.mock;

public abstract class BaseExtractorFactoryTest<T extends Connector, F extends IExtractorFactory<T>> {
	protected static final String CONNECTOR_ID = "testConnectorId";

	protected F extractorFactory;
	protected T connectorMock;
	private MutableExtractorModel model;

	@Before
	public void setUpExtractorFactory() {
		connectorMock = mock(getConnectorType());
		this.extractorFactory = createExtractorFactory();
	}

	@Before
	public void setUpExtractorModel() {
		model = new MutableExtractorModel("testExtractorConfig");
		model.addConnectorId(CONNECTOR_ID);
		model.setAttributes(new BaseRowAttribute[0]);
	}

	protected abstract F createExtractorFactory();

	protected abstract Class<T> getConnectorType();

	protected final F getExtractorFactory() {
		return extractorFactory;
	}

	protected final T getConnectorMock() {
		return connectorMock;
	}

	protected final MutableExtractorModel getModel() {
		return model;
	}
}