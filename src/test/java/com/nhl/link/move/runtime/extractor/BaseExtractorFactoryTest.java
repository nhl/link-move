package com.nhl.link.move.runtime.extractor;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import com.nhl.link.move.connect.Connector;
import com.nhl.link.move.extractor.model.MutableExtractorModel;
import com.nhl.link.move.runtime.connect.IConnectorService;
import com.nhl.link.move.runtime.extractor.BaseExtractorFactory;

public abstract class BaseExtractorFactoryTest<T extends Connector, F extends BaseExtractorFactory<T>> {
	protected static final String CONNECTOR_ID = "testConnectorId";

	private F extractorFactory;

	private T connectorMock;

	private MutableExtractorModel model;

	private IConnectorService connectorServiceMock;

	@Before
	public void setUpExtractorFactory() {
		connectorMock = mock(getConnectorType());
		connectorServiceMock = mock(IConnectorService.class);
		when(connectorServiceMock.getConnector(getConnectorType(), CONNECTOR_ID)).thenReturn(connectorMock);

		this.extractorFactory = createExtractorFactory();
	}

	@Before
	public void setUpExtratorModel() {
		model = new MutableExtractorModel("testExtractorConfig");
		model.setConnectorId(CONNECTOR_ID);
	}

	protected abstract F createExtractorFactory();

	protected abstract Class<T> getConnectorType();

	protected final F getExtractorFactory() {
		return extractorFactory;
	}

	protected final T getConnectorMock() {
		return connectorMock;
	}

	protected final IConnectorService getConnectorServiceMock() {
		return connectorServiceMock;
	}

	protected final MutableExtractorModel getModel() {
		return model;
	}

	@Test
	public void testGetConnectorType() {
		assertEquals(getConnectorType(), getExtractorFactory().getConnectorType());
	}

	@Test
	public void testCreateExtractor() throws Exception {
		extractorFactory.createExtractor(getModel());
	}
}