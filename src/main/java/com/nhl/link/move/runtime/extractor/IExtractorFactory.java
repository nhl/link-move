package com.nhl.link.move.runtime.extractor;

import com.nhl.link.move.extractor.Extractor;
import com.nhl.link.move.extractor.model.ExtractorModel;

public interface IExtractorFactory {

	Extractor createExtractor(ExtractorModel model);
}
