package com.nhl.link.move.runtime.xml;

import com.nhl.link.move.BaseRowAttribute;
import com.nhl.link.move.connect.StreamConnector;
import com.nhl.link.move.extractor.model.MutableExtractorModel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class XmlExtractorFactoryTest {

    private static final String XML = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n<test></test>";

    protected static final String CONNECTOR_ID = "testConnectorId";

    protected XmlExtractorFactory extractorFactory;
    protected StreamConnector connectorMock;
    private MutableExtractorModel model;

    @BeforeEach
    void setUpConnector() throws IOException {
        this.connectorMock = mock(StreamConnector.class);
        when(connectorMock.getInputStream(anyMap())).thenReturn(new ByteArrayInputStream(XML.getBytes()));
    }

    @BeforeEach
    void setUpExtractorFactory() {
        this.extractorFactory = new XmlExtractorFactory();
    }

    @BeforeEach
    void setUpExtractorModel() {
        model = new MutableExtractorModel("testExtractorConfig");
        model.addConnectorId(CONNECTOR_ID);
        model.setAttributes(new BaseRowAttribute[0]);
        model.addProperty(XmlExtractorFactory.XPATH_EXPRESSION_PROPERTY, "/test");
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
    public void testCreateExtractorWithEmptyXPathExpression() {
        model.clearProperties();
        assertThrows(IllegalArgumentException.class, () -> extractorFactory.createExtractor(connectorMock, model));
    }
}