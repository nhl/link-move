package com.nhl.link.move.runtime.extractor;

import com.nhl.link.move.connect.Connector;
import com.nhl.link.move.extractor.Extractor;
import com.nhl.link.move.extractor.model.ExtractorModel;
import com.nhl.link.move.extractor.model.ExtractorName;
import com.nhl.link.move.runtime.connect.IConnectorService;
import com.nhl.link.move.runtime.extractor.model.IExtractorModelService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExtractorReloaderTest {

	private ExtractorModel mockModel;
	private Extractor mockExtractor1;
	private Extractor mockExtractor2;

	private ExtractorReloader reloader;

	@BeforeEach
	public void before() {

		ExtractorName name = ExtractorName.create("l1", "n1");

		this.mockModel = mock(ExtractorModel.class);
		when(mockModel.getType()).thenReturn("t1");
		when(mockModel.getConnectorIds()).thenReturn(Collections.singleton("c1"));
		when(mockModel.getLoadedOn()).thenReturn(System.currentTimeMillis() - 1);

		IExtractorModelService mockModelService = mock(IExtractorModelService.class);
		when(mockModelService.get(name)).thenReturn(mockModel);

		IConnectorService connectorService = mock(IConnectorService.class);

		this.mockExtractor1 = mock(Extractor.class);
		this.mockExtractor2 = mock(Extractor.class);

		@SuppressWarnings("unchecked")
		IExtractorFactory<Connector> mockExtractorFactory = mock(IExtractorFactory.class);
		when(mockExtractorFactory.createExtractor(any(Connector.class), any(ExtractorModel.class)))
				.thenReturn(mockExtractor1, mockExtractor2);

		Map<String, IExtractorFactory> mockFactories = new HashMap<>();
		mockFactories.put(mockModel.getType(), mockExtractorFactory);

		this.reloader = new ExtractorReloader(mockModelService, connectorService, mockFactories, name);
	}

	@Test
	public void testGetDelegate() {

		Extractor d1 = reloader.getOrReload();
		assertSame(mockExtractor1, d1);

		Extractor d2 = reloader.getOrReload();
		assertSame(mockExtractor1, d2);

		// 'touch' model to force resync
		when(mockModel.getLoadedOn()).thenReturn(System.currentTimeMillis() + 1);

		Extractor d3 = reloader.getOrReload();
		assertSame(mockExtractor2, d3);
	}

}
