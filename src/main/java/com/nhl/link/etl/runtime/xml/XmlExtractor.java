package com.nhl.link.etl.runtime.xml;

import com.nhl.link.etl.EtlRuntimeException;
import com.nhl.link.etl.RowAttribute;
import com.nhl.link.etl.RowReader;
import com.nhl.link.etl.extract.Extractor;
import com.nhl.link.etl.extract.ExtractorParameters;
import com.nhl.link.etl.runtime.http.HttpConnector;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;

public class XmlExtractor implements Extractor {

	private final HttpConnector connector;
	private final RowAttribute[] attributes;
	private final XPathExpression expression;

	public XmlExtractor(HttpConnector connector, RowAttribute[] attributes, XPathExpression expression) {
		this.connector = connector;
		this.attributes = attributes;
		this.expression = expression;
	}

	@Override
	public RowReader getReader(ExtractorParameters parameters) {
		try {
			InputSource inputSource = new InputSource(connector.getInputStream());
			NodeList nodes = (NodeList) expression.evaluate(inputSource, XPathConstants.NODESET);
			return new XmlRowReader(attributes, nodes);
		} catch (IOException|XPathExpressionException e) {
			throw new EtlRuntimeException(e);
		}
	}
}
