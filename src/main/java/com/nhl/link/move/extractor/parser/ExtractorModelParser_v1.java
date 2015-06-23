package com.nhl.link.move.extractor.parser;

import java.util.ArrayList;
import java.util.List;

import org.w3c.dom.DOMException;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.RowAttribute;
import com.nhl.link.move.extractor.model.ContainerAwareExtractorModel;
import com.nhl.link.move.extractor.model.ExtractorModel;
import com.nhl.link.move.extractor.model.ExtractorModelContainer;
import com.nhl.link.move.extractor.model.MutableExtractorModel;
import com.nhl.link.move.extractor.model.MutableExtractorModelContainer;

/**
 * A parser of {@link ExtractorModelContainer} XML for schema version 1.
 * 
 * @since 1.4
 */
public class ExtractorModelParser_v1 implements DOMExtractorModelParser {

	public static final String NS = "http://nhl.github.io/link-move/xsd/extractor_config_1.xsd";

	@Override
	public ExtractorModelContainer parse(String location, Element xmlRoot) {

		MutableExtractorModelContainer extractors = new MutableExtractorModelContainer(location);

		try {
			doParse(extractors, xmlRoot);
		} catch (ClassNotFoundException | DOMException e) {
			throw new LmRuntimeException("Error merging config from DOM", e);
		}

		extractors.setLoadedOn(System.currentTimeMillis());
		return extractors;
	}

	protected void doParse(MutableExtractorModelContainer container, Element rootElement)
			throws ClassNotFoundException, DOMException {

		if (!"config".equals(rootElement.getNodeName())) {
			throw new LmRuntimeException("Expected <config> element, got <" + rootElement.getNodeName() + ">");
		}

		// in v.1 there is a single extractor for the entire config, with its
		// properties placed at the second level of the XML

		MutableExtractorModel extractor = new MutableExtractorModel(ExtractorModel.DEFAULT_NAME);
		doParse(rootElement, container, extractor);
		container.addExtractor(ExtractorModel.DEFAULT_NAME, new ContainerAwareExtractorModel(container, extractor));
	}

	protected void doParse(Element rootElement, MutableExtractorModelContainer container,
			MutableExtractorModel extractor) throws ClassNotFoundException, DOMException {

		NodeList nodes = rootElement.getChildNodes();
		int len = nodes.getLength();
		for (int i = 0; i < len; i++) {

			Node c = nodes.item(i);
			if (Node.ELEMENT_NODE == c.getNodeType()) {
				Element e = (Element) c;
				switch (e.getNodeName()) {
				case "type":
					// enough to set this at the container level.. extractor
					// will inherit the type
					container.setType(e.getTextContent());
					break;
				case "connectorId":
					// enough to set this at the container level.. extractor
					// will inherit the connectorId
					container.setConnectorId(e.getTextContent());
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

	protected void processProperties(Element propertiesNode, ExtractorModel extractor) throws ClassNotFoundException,
			DOMException {

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
