package com.nhl.link.move.extractor.parser;

import com.nhl.link.move.BaseRowAttribute;
import com.nhl.link.move.ClassNameResolver;
import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.RowAttribute;
import com.nhl.link.move.extractor.model.ContainerAwareExtractorModel;
import com.nhl.link.move.extractor.model.ExtractorModelContainer;
import com.nhl.link.move.extractor.model.ExtractorName;
import com.nhl.link.move.extractor.model.MutableExtractorModel;
import com.nhl.link.move.extractor.model.MutableExtractorModelContainer;
import org.w3c.dom.DOMException;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.ArrayList;
import java.util.List;

/**
 * A parser of {@link ExtractorModelContainer} XML for schema version 2.
 *
 * @since 1.4
 */
public class ExtractorModelParser_v2 implements DOMExtractorModelParser {

    public static final String NS = "http://linkmove.io/xsd/extractor_config_2.xsd";

    @Override
    public ExtractorModelContainer parse(String location, Element xmlRoot) {

        MutableExtractorModelContainer extractors = new MutableExtractorModelContainer(location);

        try {
            parseConfig(extractors, xmlRoot);
        } catch (DOMException e) {
            throw new LmRuntimeException("Error merging config from DOM", e);
        }

        extractors.setLoadedOn(System.currentTimeMillis());
        return extractors;
    }

    protected void parseConfig(MutableExtractorModelContainer container, Element configElement)
            throws DOMException {

        if (!"config".equals(configElement.getNodeName())) {
            throw new LmRuntimeException("Expected <config> element, got <" + configElement.getNodeName() + ">");
        }

        parseContainer(container, configElement.getChildNodes());
    }

    protected void parseContainer(MutableExtractorModelContainer container, NodeList configChildren)
            throws DOMException {

        int len = configChildren.getLength();
        for (int i = 0; i < len; i++) {

            Node c = configChildren.item(i);
            if (Node.ELEMENT_NODE == c.getNodeType()) {
                Element e = (Element) c;
                switch (e.getNodeName()) {
                    case "type":
                        container.setType(e.getTextContent());
                        break;
                    case "connectorId":
                        container.addConnectorId(e.getTextContent());
                        break;
                    case "connectorIds": {
                        processConnectorIds(e, container);
                        break;
                    }
                    case "extractor":
                        parseExtractor(container, e);
                        break;
                }
            }
        }
    }

    protected void parseExtractor(MutableExtractorModelContainer container, Element extractorElement)
            throws DOMException {

        String name = ExtractorName.DEFAULT_NAME;

        // find name node to be able to create extractor

        NodeList nodes = extractorElement.getChildNodes();
        int len = nodes.getLength();
        int i = 0;
        for (; i < len; i++) {
            Node c = nodes.item(i);
            if (Node.ELEMENT_NODE == c.getNodeType()) {

                if ("name".equals(c.getNodeName())) {
                    name = c.getTextContent();
                }

                break;
            }
        }

        MutableExtractorModel extractor = new MutableExtractorModel(name);

        // now that extractor is created we can add the rest of the properties
        // to it.

        for (; i < len; i++) {

            Node c = nodes.item(i);
            if (Node.ELEMENT_NODE == c.getNodeType()) {
                Element e = (Element) c;
                switch (e.getNodeName()) {
                    case "type":
                        extractor.setType(e.getTextContent());
                        break;
                    case "connectorId":
                        extractor.addConnectorId(e.getTextContent());
                        break;
                    case "connectorIds": {
                        processConnectorIds(e, extractor);
                        break;
                    }
                    case "attributes":
                        processAttributes(e, extractor);
                        break;
                    case "properties":
                        processProperties(e, extractor);
                        break;
                }
            }
        }

        container.addExtractor(extractor.getName(), new ContainerAwareExtractorModel(container, extractor));
    }

    private void processConnectorIds(Element connectorIds, MutableExtractorModel extractor) {
        NodeList nodes = connectorIds.getChildNodes();
        int len = nodes.getLength();
        for (int i = 0; i < len; i++) {

            Node c = nodes.item(i);
            if (Node.ELEMENT_NODE == c.getNodeType()) {
                Element e = (Element) c;
                if ("id".equals(e.getNodeName())) {
                    extractor.addConnectorId(e.getTextContent());
                }
            }
        }
    }

    private void processConnectorIds(Element connectorIds, MutableExtractorModelContainer container) {
        NodeList nodes = connectorIds.getChildNodes();
        int len = nodes.getLength();
        for (int i = 0; i < len; i++) {

            Node c = nodes.item(i);
            if (Node.ELEMENT_NODE == c.getNodeType()) {
                Element e = (Element) c;
                if ("connectorId".equals(e.getNodeName())) {
                    container.addConnectorId(e.getTextContent());
                }
            }
        }
    }

    protected void processAttributes(Element attributesNode, MutableExtractorModel extractor)
            throws DOMException {

        List<RowAttribute> attributes = new ArrayList<>();

        NodeList nodes = attributesNode.getChildNodes();
        int len = nodes.getLength();
        for (int i = 0; i < len; i++) {

            Node c = nodes.item(i);
            if (Node.ELEMENT_NODE == c.getNodeType()) {
                Element e = (Element) c;
                if ("attribute".equals(e.getNodeName())) {
                    processAttribute(e, attributes);
                }
            }
        }

        extractor.setAttributes(attributes.toArray(new RowAttribute[0]));
    }

    protected void processProperties(Element propertiesNode, MutableExtractorModel extractor)
            throws DOMException {

        NodeList nodes = propertiesNode.getChildNodes();
        int len = nodes.getLength();
        for (int i = 0; i < len; i++) {

            Node c = nodes.item(i);
            if (Node.ELEMENT_NODE == c.getNodeType()) {
                Element e = (Element) c;
                extractor.addProperty(e.getTagName(), e.getTextContent());
            }
        }
    }

    protected void processAttribute(Element attributeNode, List<RowAttribute> attributes)
            throws DOMException {

        Class<?> type = null;
        String source = null;
        String target = null;

        NodeList nodes = attributeNode.getChildNodes();
        int len = nodes.getLength();
        for (int i = 0; i < len; i++) {

            Node c = nodes.item(i);
            if (Node.ELEMENT_NODE == c.getNodeType()) {
                Element e = (Element) c;
                switch (e.getNodeName()) {
                    case "type":
                        // TODO: naive class loading
                        type = ClassNameResolver.typeForName(e.getTextContent());
                        break;
                    case "source":
                        source = e.getTextContent();
                        break;
                    case "target":
                        target = e.getTextContent();
                        break;
                }
            }
        }

        attributes.add(new BaseRowAttribute(type, source, target, attributes.size()));
    }
}
