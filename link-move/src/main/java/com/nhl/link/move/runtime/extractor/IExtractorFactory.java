package com.nhl.link.move.runtime.extractor;

import com.nhl.link.move.connect.Connector;
import com.nhl.link.move.extractor.Extractor;
import com.nhl.link.move.extractor.model.ExtractorModel;

public interface IExtractorFactory<T extends Connector> {

	String getExtractorType();
	Class<T> getConnectorType();
	Extractor createExtractor(T connector, ExtractorModel model);
}
