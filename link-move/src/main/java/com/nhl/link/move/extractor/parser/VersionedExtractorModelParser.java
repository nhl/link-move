package com.nhl.link.move.extractor.parser;

import java.util.Map;

import org.w3c.dom.Element;

import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.extractor.model.ExtractorModelContainer;

/**
 * A {@link DOMExtractorModelParser} that can parse multiple versions of XML
 * descriptors, provided there's a delegate configured for each unique XML
 * namespace.
 * 
 * @since 1.4
 */
public class VersionedExtractorModelParser implements DOMExtractorModelParser {

	private String defaultNS;
	private Map<String, DOMExtractorModelParser> parsersByNS;

	public VersionedExtractorModelParser(Map<String, DOMExtractorModelParser> parsersByNS, String defaultNS) {
		this.parsersByNS = parsersByNS;
		this.defaultNS = defaultNS;
	}

	@Override
	public ExtractorModelContainer parse(String location, Element xmlRoot) {
		String ns = xmlRoot.getNamespaceURI();
		return getDelegate(ns).parse(location, xmlRoot);
	}

	DOMExtractorModelParser getDelegate(String namespace) {

		String key = namespace != null ? namespace : defaultNS;

		DOMExtractorModelParser delegate = parsersByNS.get(key);
		if (delegate == null) {
			String message = namespace != null ? "Unsupported namespace: " + namespace
					: "No parser is configured for default namespace: " + defaultNS;
			throw new LmRuntimeException(message);
		}

		return delegate;
	}
}
