package com.nhl.link.etl.extract.parser;

import java.util.Map;

import org.w3c.dom.Element;

import com.nhl.link.etl.EtlRuntimeException;
import com.nhl.link.etl.extract.ExtractorConfig;

/**
 * A {@link DOMExtractorConfigParser} that can parse multiple versions of XML
 * descriptors, provided there's a delegate configured for each unique XML
 * namespace.
 * 
 * @since 1.4
 */
public class VersionedExtractorConfigParser implements DOMExtractorConfigParser {

	// use v1 namespace as the default
	static final String NO_NS_PARSER_NS = ExtractorConfigParser_1.NS;

	private Map<String, DOMExtractorConfigParser> parsersByNS;

	public VersionedExtractorConfigParser(Map<String, DOMExtractorConfigParser> parsersByNS) {
		this.parsersByNS = parsersByNS;
	}

	@Override
	public void parse(Element rootElement, ExtractorConfig config) {
		String ns = rootElement.getNamespaceURI();
		getDelegate(ns).parse(rootElement, config);
	}

	DOMExtractorConfigParser getDelegate(String namespace) {

		String key = namespace != null ? namespace : NO_NS_PARSER_NS;

		DOMExtractorConfigParser delegate = parsersByNS.get(key);
		if (delegate == null) {
			String message = namespace != null ? "Unsupported namespace: " + namespace
					: "A parser is not configured for default namespace: " + NO_NS_PARSER_NS;
			throw new EtlRuntimeException(message);
		}

		return delegate;
	}
}
