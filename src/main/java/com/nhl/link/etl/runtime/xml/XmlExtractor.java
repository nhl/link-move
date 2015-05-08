package com.nhl.link.etl.runtime.xml;

import java.io.IOException;
import java.util.Map;

import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;

import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.nhl.link.etl.EtlRuntimeException;
import com.nhl.link.etl.RowAttribute;
import com.nhl.link.etl.RowReader;
import com.nhl.link.etl.connect.StreamConnector;
import com.nhl.link.etl.extract.Extractor;

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
			throw new EtlRuntimeException(e);
		}
	}
}
