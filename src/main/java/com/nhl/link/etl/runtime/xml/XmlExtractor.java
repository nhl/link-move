package com.nhl.link.etl.runtime.xml;

import com.nhl.link.etl.EtlRuntimeException;
import com.nhl.link.etl.RowAttribute;
import com.nhl.link.etl.RowReader;
import com.nhl.link.etl.extract.Extractor;
import com.nhl.link.etl.extract.ExtractorParameters;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;

public class XmlExtractor implements Extractor {

	private final InputSource inputSource;
	private final RowAttribute[] attributes;
	private final XPathExpression expression;

	public XmlExtractor(InputSource inputSource, RowAttribute[] attributes, XPathExpression expression) {
		this.inputSource = inputSource;
		this.attributes = attributes;
		this.expression = expression;
	}

	@Override
	public RowReader getReader(ExtractorParameters parameters) {
		try {
			NodeList nodes = (NodeList) expression.evaluate(inputSource, XPathConstants.NODESET);
			return new XmlRowReader(attributes, nodes);
		} catch (XPathExpressionException e) {
			throw new EtlRuntimeException(e);
		}
	}
}
