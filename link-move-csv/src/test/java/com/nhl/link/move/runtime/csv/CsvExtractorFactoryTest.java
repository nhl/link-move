package com.nhl.link.move.runtime.csv;

import com.nhl.link.move.BaseRowAttribute;
import com.nhl.link.move.RowReader;
import com.nhl.link.move.connect.StreamConnector;
import com.nhl.link.move.extractor.model.MutableExtractorModel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CsvExtractorFactoryTest {

    protected static final String CONNECTOR_ID = "testConnectorId";

    protected CsvExtractorFactory extractorFactory;
    protected StreamConnector connectorMock;
    private MutableExtractorModel model;

    @BeforeEach
    public void setUpConnector() {
        this.connectorMock = mock(StreamConnector.class);
    }

    @BeforeEach
    public void setUpExtractorFactory() {
        this.extractorFactory = new CsvExtractorFactory();
    }

    @BeforeEach
    public void setUpExtractorModel() {
        model = new MutableExtractorModel("testExtractorConfig");
        model.addConnectorId(CONNECTOR_ID);
        model.setAttributes(new BaseRowAttribute[0]);
        model.setAttributes(new BaseRowAttribute(String.class, "k1", "k1", 0),
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
    public void testGetConnectorType() {
        assertEquals(StreamConnector.class, extractorFactory.getConnectorType());
    }

    @Test
    public void testCreateExtractor() {
        extractorFactory.createExtractor(connectorMock, model);
    }

    @Test
    public void testBasicIntegration() throws IOException {
        when(connectorMock.getInputStream(anyMap()))
                .thenReturn(new ByteArrayInputStream("r1c1,r1c2,r1c3\nr2c1,r2c2,r2c3".getBytes()));
        RowReader reader = extractorFactory.createExtractor(connectorMock, model).getReader(Collections.emptyMap());
        doCheck(reader);
    }

    @Test
    public void testSetDelimiter() throws IOException {
        when(connectorMock.getInputStream(anyMap()))
                .thenReturn(new ByteArrayInputStream("r1c1;r1c2;r1c3\nr2c1;r2c2;r2c3".getBytes()));
        model.addProperty(CsvExtractorFactory.DELIMITER_PROPERTY, ";");
        RowReader reader = extractorFactory.createExtractor(connectorMock, model).getReader(Collections.emptyMap());
        doCheck(reader);
    }

    @Test
    public void testSetReadFrom() throws IOException {
        when(connectorMock.getInputStream(anyMap()))
                .thenReturn(new ByteArrayInputStream("k1,k2,k3\nr1c1,r1c2,r1c3\nr2c1,r2c2,r2c3".getBytes()));
        model.addProperty(CsvExtractorFactory.READ_FROM_PROPERTY, "2");
        RowReader reader = extractorFactory.createExtractor(connectorMock, model).getReader(Collections.emptyMap());
        doCheck(reader);
    }
}
