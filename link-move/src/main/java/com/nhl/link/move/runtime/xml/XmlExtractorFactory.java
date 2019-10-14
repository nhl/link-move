package com.nhl.link.move.runtime.xml;

import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.RowAttribute;
import com.nhl.link.move.connect.StreamConnector;
import com.nhl.link.move.extractor.Extractor;
import com.nhl.link.move.extractor.model.ExtractorModel;
import com.nhl.link.move.runtime.extractor.IExtractorFactory;

import javax.xml.XMLConstants;
import javax.xml.namespace.NamespaceContext;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * @since 1.4
 */
public class XmlExtractorFactory implements IExtractorFactory<StreamConnector> {

    public static final String XPATH_EXPRESSION_PROPERTY = "extractor.xml.xpathexpression";
    public static final String XPATH_NS_BINDING_PROPERTY = "extractor.xml.nsbinding";
    private static final String XML_EXTRACTOR_TYPE = "xml";
    private final XPathFactory xPathFactory;

    public XmlExtractorFactory() {
        xPathFactory = XPathFactory.newInstance();
    }

    @Override
    public String getExtractorType() {
        return XML_EXTRACTOR_TYPE;
    }

    @Override
    public Class<StreamConnector> getConnectorType() {
        return StreamConnector.class;
    }

    @Override
    public Extractor createExtractor(StreamConnector connector, ExtractorModel model) {
        try {
            NamespaceContext namespaceContext = getNamespaceContext(model);
            XPathExpression expression = getXPathExpression(model, namespaceContext);
            return new XmlExtractor(connector, mapToXmlAttributes(model.getAttributes(), namespaceContext), expression);
        } catch (XPathExpressionException e) {
            throw new LmRuntimeException(e);
        }
    }

    private XPathExpression getXPathExpression(ExtractorModel model, NamespaceContext namespaceContext) throws XPathExpressionException {
        String expressionString = model.getPropertyValue(XPATH_EXPRESSION_PROPERTY);
        if (expressionString == null) {
            throw new IllegalArgumentException(format("Missing required property for key '%s'",
                    XPATH_EXPRESSION_PROPERTY));
        }
        XPath xPath = xPathFactory.newXPath();
        if (namespaceContext != null) {
            xPath.setNamespaceContext(namespaceContext);
        }
        return xPath.compile(expressionString);
    }

    private NamespaceContext getNamespaceContext(ExtractorModel model) {

        Map<String, String> prefixToNamespaceMap =
                model.getPropertyValues(XPATH_NS_BINDING_PROPERTY)
                        .stream()
                        .map(s -> {
                            String[] parts = s.split("=");
                            if (parts.length != 2) {
                                throw new LmRuntimeException("Invalid namespace binding: " + s);
                            }
                            return parts;
                        })
                        .collect(Collectors.toMap(parts -> parts[0], parts -> parts[1]));

        return new NamespaceContext() {
            @Override
            public String getNamespaceURI(String prefix) {
                return prefixToNamespaceMap.getOrDefault(prefix, XMLConstants.NULL_NS_URI);
            }

            @Override
            public String getPrefix(String namespaceURI) {
                // not used in XPath processing
                throw new UnsupportedOperationException();
            }

            @Override
            public Iterator getPrefixes(String namespaceURI) {
                return prefixToNamespaceMap.keySet().iterator();
            }
        };
    }

    private XmlRowAttribute[] mapToXmlAttributes(RowAttribute[] attributes, NamespaceContext namespaceContext) {
        int len = attributes.length;
        XmlRowAttribute[] xmlAttributes = new XmlRowAttribute[len];

        for (int i = 0; i < len; i++) {
            xmlAttributes[i] = new XmlRowAttribute(attributes[i], xPathFactory, namespaceContext);
        }
        return xmlAttributes;
    }
}
