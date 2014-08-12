package com.nhl.link.framework.etl.runtime.extract;

import com.nhl.link.framework.etl.extract.Extractor;
import com.nhl.link.framework.etl.extract.ExtractorConfig;

public interface IExtractorFactory {

	Extractor createExtractor(ExtractorConfig config);
}
