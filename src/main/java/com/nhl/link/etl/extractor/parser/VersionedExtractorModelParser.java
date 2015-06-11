package com.nhl.link.etl.extractor.parser;

import java.util.Map;

import org.w3c.dom.Element;

import com.nhl.link.etl.EtlRuntimeException;
import com.nhl.link.etl.extractor.model.ExtractorModelContainer;

/**
 * A {@link DOMExtractorModelParser} that can parse multiple versions of XML
 * descriptors, provided there's a delegate configured for each unique XML
 * namespace.
 * 
 * @since 1.4
 */
public class VersionedExtractorModelParser implements DOMExtractorModelParser {

	// use v1 namespace as the default
	static final String NO_NS_PARSER_NS = ExtractorModelParser_v1.NS;

	private Map<String, DOMExtractorModelParser> parsersByNS;

	public VersionedExtractorModelParser(Map<String, DOMExtractorModelParser> parsersByNS) {
		this.parsersByNS = parsersByNS;
	}

	@Override
	public ExtractorModelContainer parse(String location, Element xmlRoot) {
		String ns = xmlRoot.getNamespaceURI();
		return getDelegate(ns).parse(location, xmlRoot);
	}

	DOMExtractorModelParser getDelegate(String namespace) {

		String key = namespace != null ? namespace : NO_NS_PARSER_NS;

		DOMExtractorModelParser delegate = parsersByNS.get(key);
		if (delegate == null) {
			String message = namespace != null ? "Unsupported namespace: " + namespace
					: "A parser is not configured for default namespace: " + NO_NS_PARSER_NS;
			throw new EtlRuntimeException(message);
		}

		return delegate;
	}
}
