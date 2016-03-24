package com.nhl.link.move.runtime.xml;

import static java.lang.String.format;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import com.nhl.link.move.RowAttribute;
import com.nhl.link.move.runtime.extractor.IExtractorFactory;

import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.connect.StreamConnector;
import com.nhl.link.move.extractor.Extractor;
import com.nhl.link.move.extractor.model.ExtractorModel;

/**
 * @since 1.4
 */
public class XmlExtractorFactory implements IExtractorFactory<StreamConnector> {

	private static final String XML_EXTRACTOR_TYPE = "xml";
	public static final String XPATH_EXPRESSION_PROPERTY = "extractor.xml.xpathexpression";

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
			XPathExpression expression = getXPathExpression(model);
			return new XmlExtractor(connector, mapToXmlAttributes(model.getAttributes()), expression);
		} catch (XPathExpressionException e) {
			throw new LmRuntimeException(e);
		}
	}

	private XPathExpression getXPathExpression(ExtractorModel model) throws XPathExpressionException {
		String expressionString = model.getProperties().get(XPATH_EXPRESSION_PROPERTY);
		if (expressionString == null) {
			throw new IllegalArgumentException(format("Missing required property for key '%s'",
					XPATH_EXPRESSION_PROPERTY));
		}
		XPath xPath = xPathFactory.newXPath();
		return xPath.compile(expressionString);
	}

	private XmlRowAttribute[] mapToXmlAttributes(RowAttribute[] attributes) {
		int len = attributes.length;
		XmlRowAttribute[] xmlAttributes = new XmlRowAttribute[len];

		for (int i = 0; i < len; i++) {
			xmlAttributes[i] = new XmlRowAttribute(attributes[i], xPathFactory);
		}
		return xmlAttributes;
	}
}
