package com.nhl.link.etl.extractor.parser;

import org.w3c.dom.Element;

import com.nhl.link.etl.extractor.model.ExtractorModelContainer;

/**
 * An extractor config parser that converts a DOM tree into
 * {@link ExtractorModelContainer}.
 * 
 * @since 1.4
 */
public interface DOMExtractorModelParser {

	ExtractorModelContainer parse(String location, Element xmlRoot);
}
