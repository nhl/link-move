package com.nhl.link.etl.runtime.extractor.loader;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;

import com.nhl.link.etl.EtlRuntimeException;

/**
 * A default {@link IExtractorConfigLoader} that loads XML files from CLASSPATH.
 */
public class ClasspathExtractorConfigLoader extends BaseExtractorConfigLoader {

	@Override
	protected Reader getXmlSource(String name) throws IOException {

		if (!name.endsWith(".xml")) {
			name = name + ".xml";
		}

		URL resource = ClasspathExtractorConfigLoader.class.getClassLoader().getResource(name);
		if (resource == null) {
			throw new EtlRuntimeException("Extractor config not found in classpath: " + name);
		}

		return new InputStreamReader(resource.openStream(), "UTF-8");
	}

	@Override
	public boolean needsReload(String name, long lastSeen) {
		return lastSeen <= 0;
	}
}
