package com.nhl.link.move.runtime.extractor;

import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.nhl.link.move.extractor.Extractor;
import com.nhl.link.move.extractor.model.ExtractorModel;
import com.nhl.link.move.extractor.model.ExtractorName;
import com.nhl.link.move.runtime.extractor.IExtractorFactory;
import com.nhl.link.move.runtime.extractor.ReloadableExtractor;
import com.nhl.link.move.runtime.extractor.model.IExtractorModelService;

public class ReloadableExtractorTest {

	private IExtractorModelService mockModelService;
	private ExtractorName name;
	private ExtractorModel mockModel;
	private Map<String, IExtractorFactory> mockFactories;
	private Extractor mockExtractor1;
	private Extractor mockExtractor2;
	private Extractor mockExtractor3;

	private ReloadableExtractor extractor;

	@Before
	public void before() {

		this.name = ExtractorName.create("l1", "n1");

		this.mockModel = mock(ExtractorModel.class);
		when(mockModel.getType()).thenReturn("t1");
		when(mockModel.getLoadedOn()).thenReturn(System.currentTimeMillis() - 1);

		this.mockModelService = mock(IExtractorModelService.class);
		when(mockModelService.get(name)).thenReturn(mockModel);

		this.mockExtractor1 = mock(Extractor.class);
		this.mockExtractor2 = mock(Extractor.class);
		this.mockExtractor3 = mock(Extractor.class);

		IExtractorFactory mockExtractorFactory = mock(IExtractorFactory.class);
		when(mockExtractorFactory.createExtractor(any(ExtractorModel.class))).thenReturn(mockExtractor1,
				mockExtractor2, mockExtractor3);

		this.mockFactories = new HashMap<>();
		mockFactories.put(mockModel.getType(), mockExtractorFactory);

		this.extractor = new ReloadableExtractor(mockModelService, mockFactories, name);
	}

	@Test
	public void testGetDelegate() {

		Extractor d1 = extractor.getDelegate();
		assertSame(mockExtractor1, d1);

		Extractor d2 = extractor.getDelegate();
		assertSame(mockExtractor1, d2);

		// 'touch' model to force resync
		when(mockModel.getLoadedOn()).thenReturn(System.currentTimeMillis() + 1);

		Extractor d3 = extractor.getDelegate();
		assertSame(mockExtractor2, d3);
	}

}
