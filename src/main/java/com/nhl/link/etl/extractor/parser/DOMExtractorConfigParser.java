package com.nhl.link.etl.extractor.parser;

import org.w3c.dom.Element;

import com.nhl.link.etl.extractor.ExtractorConfig;

/**
 * An extractor config parser that converts a DOM tree into
 * {@link ExtractorConfig}.
 * 
 * @since 1.4
 */
public interface DOMExtractorConfigParser {

	void parse(Element rootElement, ExtractorConfig config);
}
