package com.nhl.link.etl.runtime.extractor;

import com.nhl.link.etl.extractor.Extractor;
import com.nhl.link.etl.extractor.model.ExtractorModel;

public interface IExtractorFactory {

	Extractor createExtractor(ExtractorModel model);
}
