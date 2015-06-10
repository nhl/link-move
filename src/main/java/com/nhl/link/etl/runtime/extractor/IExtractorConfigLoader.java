package com.nhl.link.etl.runtime.extractor;

import com.nhl.link.etl.extractor.ExtractorConfig;

public interface IExtractorConfigLoader {

	ExtractorConfig loadConfig(String name);
	
	boolean needsReload(String name, long lastSeen);
}
