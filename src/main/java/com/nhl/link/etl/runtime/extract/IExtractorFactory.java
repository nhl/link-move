package com.nhl.link.etl.runtime.extract;

import com.nhl.link.etl.extract.Extractor;
import com.nhl.link.etl.extract.ExtractorConfig;

public interface IExtractorFactory {

	Extractor createExtractor(ExtractorConfig config);
}
