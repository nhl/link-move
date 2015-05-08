package com.nhl.link.etl.runtime.xml;

import com.nhl.link.etl.EtlRuntimeException;
import com.nhl.link.etl.connect.StreamConnector;
import com.nhl.link.etl.extract.Extractor;
import com.nhl.link.etl.extract.ExtractorConfig;
import com.nhl.link.etl.runtime.connect.IConnectorService;
import com.nhl.link.etl.runtime.extract.BaseExtractorFactory;
import org.apache.cayenne.di.Inject;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import static java.lang.String.format;

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
	protected Extractor createExtractor(StreamConnector connector, ExtractorConfig config) {
		try {
			XPathExpression expression = getXPathExpression(config);
			return new XmlExtractor(connector, config.getAttributes(), expression);
		} catch (XPathExpressionException e) {
			throw new EtlRuntimeException(e);
		}
	}

	private XPathExpression getXPathExpression(ExtractorConfig config) throws XPathExpressionException {
		String expressionString = config.getProperties().get(XPATH_EXPRESSION_PROPERTY);
		if (expressionString == null) {
			throw new IllegalArgumentException(
					format("Missing required property for key '%s'", XPATH_EXPRESSION_PROPERTY));
		}
		XPath xPath = xPathFactory.newXPath();
		return xPath.compile(expressionString);
	}
}
