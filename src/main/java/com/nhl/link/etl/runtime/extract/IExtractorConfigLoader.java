package com.nhl.link.etl.runtime.extract;

import com.nhl.link.etl.extract.ExtractorConfig;

public interface IExtractorConfigLoader {

	ExtractorConfig loadConfig(String name);
	
	boolean needsReload(String name, long lastSeen);
}
