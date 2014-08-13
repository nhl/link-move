package com.nhl.link.etl.extract;

import com.nhl.link.etl.RowReader;

/**
 * Defines a reusable object responsible for the "extract" step of a given ETL
 * task. It provides a stream of source data based on some internal query
 * template and a set of parameters passed by the caller.
 */
public interface Extractor {

	RowReader getReader(ExtractorParameters parameters);
}
