package com.nhl.link.etl.runtime.extractor;

import com.nhl.link.etl.extractor.Extractor;
import com.nhl.link.etl.extractor.ExtractorConfig;

public interface IExtractorFactory {

	Extractor createExtractor(ExtractorConfig config);
}
