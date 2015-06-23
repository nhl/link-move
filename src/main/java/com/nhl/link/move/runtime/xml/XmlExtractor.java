package com.nhl.link.move.runtime.xml;

import java.io.IOException;
import java.util.Map;

import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;

import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.RowAttribute;
import com.nhl.link.move.RowReader;
import com.nhl.link.move.connect.StreamConnector;
import com.nhl.link.move.extractor.Extractor;

/**
 * @since 1.4
 */
public class XmlExtractor implements Extractor {

	private final StreamConnector connector;
	private final RowAttribute[] attributes;
	private final XPathExpression expression;

	public XmlExtractor(StreamConnector connector, RowAttribute[] attributes, XPathExpression expression) {
		this.connector = connector;
		this.attributes = attributes;
		this.expression = expression;
	}

	@Override
	public RowReader getReader(Map<String, ?> parameters) {
		try {
			InputSource inputSource = new InputSource(connector.getInputStream());
			NodeList nodes = (NodeList) expression.evaluate(inputSource, XPathConstants.NODESET);
			return new XmlRowReader(attributes, nodes);
		} catch (IOException | XPathExpressionException e) {
			throw new LmRuntimeException(e);
		}
	}
}
