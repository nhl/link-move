package com.nhl.link.framework.etl.runtime.extract;

import com.nhl.link.framework.etl.extract.ExtractorConfig;

public interface IExtractorConfigLoader {

	ExtractorConfig loadConfig(String name);
	
	boolean needsReload(String name, long lastSeen);
}
