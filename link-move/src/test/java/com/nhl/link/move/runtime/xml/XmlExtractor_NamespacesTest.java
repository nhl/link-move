package com.nhl.link.move.runtime.xml;

import com.nhl.link.move.BaseRowAttribute;
import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.Row;
import com.nhl.link.move.connect.StreamConnector;
import com.nhl.link.move.extractor.Extractor;
import com.nhl.link.move.extractor.model.MutableExtractorModel;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertEquals;

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

        List<Row> rows = readRows(extractor);
        assertEquals(1, rows.size());

        Row result = rows.iterator().next();
        assertEquals("x", result.get(result.attributes()[0]));
        assertEquals("y", result.get(result.attributes()[1]));
        assertEquals("z", result.get(result.attributes()[2]));
    }

    @Test
    public void testXmlExtractor_Namespaces_InvalidBinding() {
        Extractor extractor = createExtractor("/doc/ns1:e1", Collections.singletonMap("ns1", "http://www.example.org"));

        List<Row> rows = readRows(extractor);
        assertEquals(0, rows.size());
    }

    @Test(expected = LmRuntimeException.class)
    public void testXmlExtractor_Namespaces_NoBindings() {
        createExtractor("/doc/ns1:e1", Collections.emptyMap());
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
            public InputStream getInputStream() throws IOException {
                return new ByteArrayInputStream(xmlDocument.getBytes("UTF-8"));
            }

            @Override
            public void shutdown() {
                // do nothing
            }
        };
    }

    private List<Row> readRows(Extractor extractor) {
        return StreamSupport.stream(extractor.getReader(Collections.emptyMap()).spliterator(), false)
                            .collect(Collectors.toList());
    }
}
