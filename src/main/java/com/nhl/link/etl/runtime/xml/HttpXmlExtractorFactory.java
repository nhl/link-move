package com.nhl.link.etl.runtime.xml;

import com.nhl.link.etl.EtlRuntimeException;
import com.nhl.link.etl.extract.Extractor;
import com.nhl.link.etl.extract.ExtractorConfig;
import com.nhl.link.etl.runtime.connect.IConnectorService;
import com.nhl.link.etl.runtime.extract.BaseExtractorFactory;
import com.nhl.link.etl.runtime.http.IHttpConnector;
import org.apache.cayenne.di.Inject;
import org.xml.sax.InputSource;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.IOException;

import static java.lang.String.format;

public class HttpXmlExtractorFactory extends BaseExtractorFactory<IHttpConnector> {
	public static final String XPATH_EXPRESSION_PROPERTY = "extractor.xml.xpathexpression";

	private final XPathFactory xPathFactory;

	public HttpXmlExtractorFactory(@Inject IConnectorService connectorService) {
		super(connectorService);
		xPathFactory = XPathFactory.newInstance();
	}

	@Override
	protected Class<IHttpConnector> getConnectorType() {
		return IHttpConnector.class;
	}

	@Override
	protected Extractor createExtractor(IHttpConnector connector, ExtractorConfig config) {
		try {
			InputSource inputSource = new InputSource(connector.getInputStream());
			XPathExpression expression = getXPathExpression(config);
			return new XmlExtractor(inputSource, config.getAttributes(), expression);
		} catch (IOException | XPathExpressionException e) {
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
