package com.nhl.link.move.runtime.xml;

import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.RowReader;
import com.nhl.link.move.connect.StreamConnector;
import com.nhl.link.move.extractor.Extractor;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.util.Map;

/**
 * @since 1.4
 */
public class XmlExtractor implements Extractor {

	private final StreamConnector connector;
	private final XmlRowAttribute[] rowHeader;
	private final XPathExpression expression;

	public XmlExtractor(StreamConnector connector, XmlRowAttribute[] rowHeader, XPathExpression expression) {
		this.connector = connector;
		this.rowHeader = rowHeader;
		this.expression = expression;
	}

	@Override
	public RowReader getReader(Map<String, ?> parameters) {
		try {
			InputSource inputSource = new InputSource(connector.getInputStream(parameters));
			NodeList nodes = (NodeList) expression.evaluate(inputSource, XPathConstants.NODESET);
			return new XmlRowReader(rowHeader, nodes);
		} catch (IOException | XPathExpressionException e) {
			throw new LmRuntimeException(e);
		}
	}
}
