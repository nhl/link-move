package com.nhl.link.move.extractor.parser;

import java.util.ArrayList;
import java.util.List;

import org.w3c.dom.DOMException;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.nhl.link.move.EtlRuntimeException;
import com.nhl.link.move.RowAttribute;
import com.nhl.link.move.extractor.model.ContainerAwareExtractorModel;
import com.nhl.link.move.extractor.model.ExtractorModel;
import com.nhl.link.move.extractor.model.ExtractorModelContainer;
import com.nhl.link.move.extractor.model.MutableExtractorModel;
import com.nhl.link.move.extractor.model.MutableExtractorModelContainer;

/**
 * A parser of {@link ExtractorModelContainer} XML for schema version 2.
 * 
 * @since 1.4
 */
public class ExtractorModelParser_v2 implements DOMExtractorModelParser {

	public static final String NS = "http://nhl.github.io/link-move/xsd/extractor_config_2.xsd";

	@Override
	public ExtractorModelContainer parse(String location, Element xmlRoot) {

		MutableExtractorModelContainer extractors = new MutableExtractorModelContainer(location);

		try {
			parseConfig(extractors, xmlRoot);
		} catch (ClassNotFoundException | DOMException e) {
			throw new EtlRuntimeException("Error merging config from DOM", e);
		}

		extractors.setLoadedOn(System.currentTimeMillis());
		return extractors;
	}

	protected void parseConfig(MutableExtractorModelContainer container, Element configElement)
			throws ClassNotFoundException, DOMException {

		if (!"config".equals(configElement.getNodeName())) {
			throw new EtlRuntimeException("Expected <config> element, got <" + configElement.getNodeName() + ">");
		}

		parseContainer(container, configElement.getChildNodes());
	}

	protected void parseContainer(MutableExtractorModelContainer container, NodeList configChildren)
			throws ClassNotFoundException, DOMException {

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
					container.setConnectorId(e.getTextContent());
					break;

				case "extractor":
					parseExtractor(container, e);
					break;
				}
			}
		}
	}

	protected void parseExtractor(MutableExtractorModelContainer container, Element extractorElement)
			throws ClassNotFoundException, DOMException {

		String name = ExtractorModel.DEFAULT_NAME;

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
					extractor.setConnectorId(e.getTextContent());
					break;
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

	protected void processAttributes(Element attributesNode, MutableExtractorModel extractor)
			throws ClassNotFoundException, DOMException {

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

		extractor.setAttributes(attributes.toArray(new RowAttribute[attributes.size()]));
	}

	protected void processProperties(Element propertiesNode, MutableExtractorModel extractor)
			throws ClassNotFoundException, DOMException {

		NodeList nodes = propertiesNode.getChildNodes();
		int len = nodes.getLength();
		for (int i = 0; i < len; i++) {

			Node c = nodes.item(i);
			if (Node.ELEMENT_NODE == c.getNodeType()) {
				Element e = (Element) c;
				extractor.getProperties().put(e.getTagName(), e.getTextContent());
			}
		}
	}

	protected void processAttribute(Element attributeNode, List<RowAttribute> attributes)
			throws ClassNotFoundException, DOMException {

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
					type = ParserUtil.typeForName(e.getTextContent());
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

		attributes.add(new RowAttribute(type, source, target, attributes.size()));
	}
}
