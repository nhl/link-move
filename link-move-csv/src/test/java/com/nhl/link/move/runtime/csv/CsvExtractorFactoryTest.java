package com.nhl.link.move.runtime.csv;

import com.nhl.link.move.BaseRowAttribute;
import com.nhl.link.move.RowReader;
import com.nhl.link.move.connect.StreamConnector;
import com.nhl.link.move.runtime.extractor.BaseExtractorFactoryTest;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Iterator;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.when;

public class CsvExtractorFactoryTest extends BaseExtractorFactoryTest<StreamConnector, CsvExtractorFactory> {

	@Override
	protected CsvExtractorFactory createExtractorFactory() {
		return new CsvExtractorFactory();
	}

	@Override
	protected Class<StreamConnector> getConnectorType() {
		return StreamConnector.class;
	}

	@Override
	public void setUpExtractorModel() {
		super.setUpExtractorModel();
		getModel().setAttributes(new BaseRowAttribute(String.class, "k1", "k1", 0),
				new BaseRowAttribute(String.class, "k2", "k2", 1),
				new BaseRowAttribute(String.class, "k3", "k3", 2));
	}

	private void doCheck(RowReader reader) {
		Iterator<Object[]> rowIt = reader.iterator();
		for (int i = 1; i <= 2; i++) {
			assertTrue(rowIt.hasNext());
			Object[] row = rowIt.next();
			for (int j = 1; j <= 3; j++) {
				assertEquals("r" + i + "c" + j, row[j - 1]);
			}
		}
		assertFalse(rowIt.hasNext());
	}

	@Test
	public void testBasicIntegration() throws IOException {
		when(getConnectorMock().getInputStream(anyMap()))
				.thenReturn(new ByteArrayInputStream("r1c1,r1c2,r1c3\nr2c1,r2c2,r2c3".getBytes()));
		RowReader reader = getExtractorFactory().createExtractor(getConnectorMock(), getModel()).getReader(null);
		doCheck(reader);
	}

	@Test
	public void testSetDelimiter() throws IOException {
		when(getConnectorMock().getInputStream(anyMap()))
				.thenReturn(new ByteArrayInputStream("r1c1;r1c2;r1c3\nr2c1;r2c2;r2c3".getBytes()));
		getModel().getProperties().put(CsvExtractorFactory.DELIMITER_PROPERTY, ";");
		RowReader reader = getExtractorFactory().createExtractor(getConnectorMock(), getModel()).getReader(null);
		doCheck(reader);
	}

	@Test
	public void testSetReadFrom() throws IOException {
		when(getConnectorMock().getInputStream(anyMap()))
				.thenReturn(new ByteArrayInputStream("k1,k2,k3\nr1c1,r1c2,r1c3\nr2c1,r2c2,r2c3".getBytes()));
		getModel().getProperties().put(CsvExtractorFactory.READ_FROM_PROPERTY, "2");
		RowReader reader = getExtractorFactory().createExtractor(getConnectorMock(), getModel()).getReader(null);
		doCheck(reader);
	}
}
