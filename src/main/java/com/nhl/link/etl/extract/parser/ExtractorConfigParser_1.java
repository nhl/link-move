package com.nhl.link.etl.extract.parser;

import java.util.ArrayList;
import java.util.List;

import org.w3c.dom.DOMException;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.nhl.link.etl.EtlRuntimeException;
import com.nhl.link.etl.RowAttribute;
import com.nhl.link.etl.extract.ExtractorConfig;

/**
 * A parser of {@link ExtractorConfig} XML for schema version 1.
 * 
 * @since 1.4
 */
public class ExtractorConfigParser_1 implements DOMExtractorConfigParser {

	public static final String NS = "http://nhl.github.io/link-etl/xsd/extractor_config_1.xsd";

	@Override
	public void parse(Element rootElement, ExtractorConfig config) {
		try {
			doParse(rootElement, config);
		} catch (ClassNotFoundException | DOMException e) {
			throw new EtlRuntimeException("Error merging config from DOM", e);
		}
	}

	protected void doParse(Element rootElement, ExtractorConfig config) throws ClassNotFoundException, DOMException {

		if (!"config".equals(rootElement.getNodeName())) {
			throw new EtlRuntimeException("Expected <config> element, got <" + rootElement.getNodeName() + ">");
		}

		NodeList nodes = rootElement.getChildNodes();
		int len = nodes.getLength();
		for (int i = 0; i < len; i++) {

			Node c = nodes.item(i);
			if (Node.ELEMENT_NODE == c.getNodeType()) {
				Element e = (Element) c;
				switch (e.getNodeName()) {
				case "type":
					config.setType(e.getTextContent());
					break;
				case "connectorId":
					config.setConnectorId(e.getTextContent());
					break;
				case "attributes":
					processAttributes(e, config);
					break;
				case "properties":
					processProperties(e, config);
					break;
				}
			}
		}
	}

	protected void processAttributes(Element attributesNode, ExtractorConfig config) throws ClassNotFoundException,
			DOMException {

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

		config.setAttributes(attributes.toArray(new RowAttribute[attributes.size()]));
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
					type = getType(e.getTextContent());
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

	protected static Class<?> getType(String className) throws ClassNotFoundException {
		try {
			return Class.forName(className);
		} catch (ClassNotFoundException e) {

			// copied from Cayenne DefaultAdhocObjectFactory, except for inner
			// classes and VOID that we will not support (yet?)

			if (!className.endsWith("[]")) {
				if ("byte".equals(className)) {
					return Byte.TYPE;
				} else if ("int".equals(className)) {
					return Integer.TYPE;
				} else if ("short".equals(className)) {
					return Short.TYPE;
				} else if ("char".equals(className)) {
					return Character.TYPE;
				} else if ("double".equals(className)) {
					return Double.TYPE;
				} else if ("long".equals(className)) {
					return Long.TYPE;
				} else if ("float".equals(className)) {
					return Float.TYPE;
				} else if ("boolean".equals(className)) {
					return Boolean.TYPE;
				}

				throw e;
			}

			if (className.length() < 3) {
				throw new IllegalArgumentException("Invalid class name: " + className);
			}

			// TODO: support for multi-dim arrays
			className = className.substring(0, className.length() - 2);

			if ("byte".equals(className)) {
				return byte[].class;
			} else if ("int".equals(className)) {
				return int[].class;
			} else if ("long".equals(className)) {
				return long[].class;
			} else if ("short".equals(className)) {
				return short[].class;
			} else if ("char".equals(className)) {
				return char[].class;
			} else if ("double".equals(className)) {
				return double[].class;
			} else if ("float".equals(className)) {
				return float[].class;
			} else if ("boolean".equals(className)) {
				return boolean[].class;
			} else {
				// Object[]?
				return Class.forName("[L" + className + ";");
			}
		}
	}

	protected void processProperties(Element propertiesNode, ExtractorConfig config) throws ClassNotFoundException,
			DOMException {

		NodeList nodes = propertiesNode.getChildNodes();
		int len = nodes.getLength();
		for (int i = 0; i < len; i++) {

			Node c = nodes.item(i);
			if (Node.ELEMENT_NODE == c.getNodeType()) {
				Element e = (Element) c;
				config.getProperties().put(e.getTagName(), e.getTextContent());
			}
		}
	}

}
