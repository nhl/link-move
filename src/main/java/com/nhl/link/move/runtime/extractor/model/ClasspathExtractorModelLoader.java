package com.nhl.link.move.runtime.extractor.model;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;

import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.extractor.model.ExtractorModelContainer;

/**
 * A default {@link IExtractorModelLoader} that loads XML files from CLASSPATH.
 */
public class ClasspathExtractorModelLoader extends BaseExtractorModelLoader {

	@Override
	protected Reader getXmlSource(String name) throws IOException {

		if (!name.endsWith(".xml")) {
			name = name + ".xml";
		}

		URL resource = ClasspathExtractorModelLoader.class.getClassLoader().getResource(name);
		if (resource == null) {
			throw new LmRuntimeException("Extractor config not found in classpath: " + name);
		}

		return new InputStreamReader(resource.openStream(), "UTF-8");
	}

	@Override
	public boolean needsReload(ExtractorModelContainer container) {
		return false;
	}
}
