package com.nhl.link.move.extractor.parser;

import com.nhl.link.move.extractor.model.ExtractorModelContainer;

import java.io.Reader;

/**
 * @since 2.4
 */
public interface IExtractorModelParser {
    
    ExtractorModelContainer parse(String name, Reader in);
}
