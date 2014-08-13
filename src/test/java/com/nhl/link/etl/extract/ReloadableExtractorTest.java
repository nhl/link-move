package com.nhl.link.etl.extract;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.nhl.link.etl.extract.Extractor;
import com.nhl.link.etl.extract.ExtractorConfig;
import com.nhl.link.etl.extract.ReloadableExtractor;
import com.nhl.link.etl.runtime.extract.IExtractorConfigLoader;
import com.nhl.link.etl.runtime.extract.IExtractorFactory;

public class ReloadableExtractorTest {

	@Test
	public void testGetDelegate() {

		ExtractorConfig config = new ExtractorConfig("x");
		config.setType("xType");

		IExtractorConfigLoader configLoader = mock(IExtractorConfigLoader.class);
		when(configLoader.needsReload(anyString(), anyLong())).thenReturn(false);
		when(configLoader.loadConfig(anyString())).thenReturn(config);

		IExtractorFactory extractorFactory = mock(IExtractorFactory.class);
		when(extractorFactory.createExtractor(any(ExtractorConfig.class))).thenReturn(mock(Extractor.class),
				mock(Extractor.class), mock(Extractor.class), mock(Extractor.class), mock(Extractor.class));

		Map<String, IExtractorFactory> factories = new HashMap<>();
		factories.put(config.getType(), extractorFactory);

		ReloadableExtractor extractor = new ReloadableExtractor(configLoader, factories, "x");

		Extractor d1 = extractor.getDelegate();
		assertNotNull(d1);

		Extractor d2 = extractor.getDelegate();
		assertSame(d1, d2);
		
		when(configLoader.needsReload(anyString(), anyLong())).thenReturn(true);

		Extractor d3 = extractor.getDelegate();
		assertNotNull(d3);
		assertNotSame(d1, d3);
	}

}
