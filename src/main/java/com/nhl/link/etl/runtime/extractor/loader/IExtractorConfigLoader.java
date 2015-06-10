package com.nhl.link.etl.runtime.extractor.loader;

import com.nhl.link.etl.extractor.ExtractorConfig;

public interface IExtractorConfigLoader {

	ExtractorConfig loadConfig(String name);
	
	boolean needsReload(String name, long lastSeen);
}
