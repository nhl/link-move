package com.nhl.link.etl.runtime.extractor;

import com.nhl.link.etl.connect.Connector;
import com.nhl.link.etl.extractor.ExtractorConfig;
import com.nhl.link.etl.runtime.connect.IConnectorService;
import com.nhl.link.etl.runtime.extractor.BaseExtractorFactory;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class BaseExtractorFactoryTest<T extends Connector, F extends BaseExtractorFactory<T>> {
	protected static final String CONNECTOR_ID = "testConnectorId";

	private F extractorFactory;

	private T connectorMock;

	private ExtractorConfig extractorConfig;

	private IConnectorService connectorServiceMock;

	@Before
	public void setUpExtractorFactory() {
		connectorMock = mock(getConnectorType());
		connectorServiceMock = mock(IConnectorService.class);
		when(connectorServiceMock.getConnector(getConnectorType(), CONNECTOR_ID))
				.thenReturn(connectorMock);
		extractorFactory = createExtractorFactory();
	}

	@Before
	public void setUpExtractorConfig() {
		extractorConfig = new ExtractorConfig("testExtractorConfig");
		extractorConfig.setConnectorId(CONNECTOR_ID);
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

	protected final ExtractorConfig getExtractorConfig() {
		return extractorConfig;
	}

	@Test
	public void testGetConnectorType() {
		assertEquals(getConnectorType(), getExtractorFactory().getConnectorType());
	}

	@Test
	public void testCreateExtractor() throws Exception {
		extractorFactory.createExtractor(getExtractorConfig());
	}
}