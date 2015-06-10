package com.nhl.link.etl.extractor;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.nhl.link.etl.CollectionRowReader;
import com.nhl.link.etl.Row;
import com.nhl.link.etl.RowReader;
import com.nhl.link.etl.extractor.Extractor;
import com.nhl.link.etl.extractor.MultiExtractor;

public class MultiExtractorTest {

	@Test
	public void testGetReader_Empty() {
		MultiExtractor e = new MultiExtractor(Collections.<Extractor> emptyList());
		assertFound(e, 0);
	}

	@Test
	public void testGetReader_One() {
		int[] closeCounter = new int[1];

		List<Extractor> extractors = new ArrayList<>();
		extractors.add(makeExtractor(2, closeCounter));

		MultiExtractor e = new MultiExtractor(extractors);
		assertFound(e, 2);
		assertEquals(1, closeCounter[0]);
	}

	@Test
	public void testGetReader_Many() {

		int[] closeCounter = new int[1];

		List<Extractor> extractors = new ArrayList<>();
		extractors.add(makeExtractor(2, closeCounter));
		extractors.add(makeExtractor(3, closeCounter));
		extractors.add(makeExtractor(4, closeCounter));

		MultiExtractor e = new MultiExtractor(extractors);
		assertFound(e, 9);
		assertEquals(3, closeCounter[0]);
	}

	@Test
	public void testGetReader_Many_FirstEmpty() {

		int[] closeCounter = new int[1];

		List<Extractor> extractors = new ArrayList<>();
		extractors.add(makeExtractor(0, closeCounter));
		extractors.add(makeExtractor(3, closeCounter));
		extractors.add(makeExtractor(4, closeCounter));

		MultiExtractor e = new MultiExtractor(extractors);
		assertFound(e, 7);
		assertEquals(3, closeCounter[0]);
	}

	protected void assertFound(MultiExtractor extractor, int expectedRows) {

		int rows = 0;
		try (RowReader reader = extractor.getReader(Collections.<String, Object> emptyMap())) {
			for (@SuppressWarnings("unused")
			Row row : reader) {
				rows++;
			}
		}

		assertEquals(expectedRows, rows);

	}

	protected Extractor makeExtractor(final int rowsToReturn, final int[] closeCounter) {
		return new Extractor() {

			@Override
			public RowReader getReader(Map<String, ?> parameters) {
				Collection<Row> rows = new ArrayList<>(rowsToReturn);
				for (int i = 0; i < rowsToReturn; i++) {
					rows.add(mock(Row.class));
				}
				return new CollectionRowReader(rows) {
					@Override
					public void close() {
						closeCounter[0]++;
					}
				};
			}
		};
	}
}
