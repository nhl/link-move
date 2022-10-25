package com.nhl.link.move.runtime.xml;

import com.nhl.link.move.BaseRowAttribute;
import com.nhl.link.move.Execution;
import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.RowReader;
import com.nhl.link.move.connect.StreamConnector;
import com.nhl.link.move.extractor.Extractor;
import com.nhl.link.move.extractor.model.MutableExtractorModel;
import com.nhl.link.move.log.LmExecutionLogger;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class XmlExtractor_NamespacesTest {

    private static final String xmlDocument =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                    "<doc xmlns:ns1=\"http://example.org/\">" +
                    "<ns1:e1>" +
                    "<a>x</a>" +
                    "<ns1:b>y</ns1:b>" +
                    "<c>z</c>" +
                    "</ns1:e1>" +
                    "</doc>";

    @Test
    public void testXmlExtractor_Namespaces() {
        Extractor extractor = createExtractor("/doc/ns1:e1", Collections.singletonMap("ns1", "http://example.org/"));

        List<Object[]> rows = readRows(extractor);
        assertEquals(1, rows.size());

        Object[] result = rows.iterator().next();
        assertEquals("x", result[0]);
        assertEquals("y", result[1]);
        assertEquals("z", result[2]);
    }

    @Test
    public void testXmlExtractor_Namespaces_InvalidBinding() {
        Extractor extractor = createExtractor("/doc/ns1:e1", Collections.singletonMap("ns1", "http://www.example.org"));

        List<Object[]> rows = readRows(extractor);
        assertEquals(0, rows.size());
    }

    @Test
    public void testXmlExtractor_Namespaces_NoBindings() {
        assertThrows(LmRuntimeException.class, () -> createExtractor("/doc/ns1:e1", Collections.emptyMap()));
    }

    private static Extractor createExtractor(String xPath, Map<String, String> namespaceBindings) {
        MutableExtractorModel model = new MutableExtractorModel("test");
        model.addConnectorId("id");
        model.addProperty(XmlExtractorFactory.XPATH_EXPRESSION_PROPERTY, xPath);
        namespaceBindings.forEach((prefix, namespace) ->
                model.addProperty(XmlExtractorFactory.XPATH_NS_BINDING_PROPERTY, prefix + "=" + namespace));

        BaseRowAttribute a1 = new BaseRowAttribute(String.class, "a", "a", 1);
        BaseRowAttribute a2 = new BaseRowAttribute(String.class, "ns1:b", "b", 2);
        BaseRowAttribute a3 = new BaseRowAttribute(String.class, "c", "c", 3);
        model.setAttributes(new BaseRowAttribute[]{a1, a2, a3});

        return new XmlExtractorFactory().createExtractor(createConnector(xmlDocument), model);
    }

    private static StreamConnector createConnector(String xmlDocument) {
        return new StreamConnector() {

            @Override
            public InputStream getInputStream(Map<String, ?> parameters) {
                return new ByteArrayInputStream(xmlDocument.getBytes(StandardCharsets.UTF_8));
            }

            @Override
            public void shutdown() {
                // do nothing
            }
        };
    }

    private List<Object[]> readRows(Extractor extractor) {
        Execution exec = mock(Execution.class);
        when(exec.getParameters()).thenReturn(Collections.emptyMap());
        when(exec.getLogger()).thenReturn(mock(LmExecutionLogger.class));

        RowReader reader = extractor.getReader(exec);

        return StreamSupport.stream(reader.spliterator(), false).collect(Collectors.toList());
    }
}
