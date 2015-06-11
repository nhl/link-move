package com.nhl.link.etl.runtime.xml;

import static java.lang.String.format;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.cayenne.di.Inject;

import com.nhl.link.etl.EtlRuntimeException;
import com.nhl.link.etl.connect.StreamConnector;
import com.nhl.link.etl.extractor.Extractor;
import com.nhl.link.etl.extractor.model.ExtractorModel;
import com.nhl.link.etl.runtime.connect.IConnectorService;
import com.nhl.link.etl.runtime.extractor.BaseExtractorFactory;

/**
 * @since 1.4
 */
public class XmlExtractorFactory extends BaseExtractorFactory<StreamConnector> {
	public static final String XPATH_EXPRESSION_PROPERTY = "extractor.xml.xpathexpression";

	private final XPathFactory xPathFactory;

	public XmlExtractorFactory(@Inject IConnectorService connectorService) {
		super(connectorService);
		xPathFactory = XPathFactory.newInstance();
	}

	@Override
	protected Class<StreamConnector> getConnectorType() {
		return StreamConnector.class;
	}

	@Override
	protected Extractor createExtractor(StreamConnector connector, ExtractorModel model) {
		try {
			XPathExpression expression = getXPathExpression(model);
			return new XmlExtractor(connector, model.getAttributes(), expression);
		} catch (XPathExpressionException e) {
			throw new EtlRuntimeException(e);
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
}
