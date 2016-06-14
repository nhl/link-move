package com.nhl.link.move.runtime.extractor.model;

import com.nhl.link.move.extractor.model.ExtractorModelContainer;
import com.nhl.link.move.runtime.LmRuntimeBuilder;
import org.apache.cayenne.di.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;

public class URLExtractorModelLoader extends BaseExtractorModelLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(URLExtractorModelLoader.class);

	private URL rootUrl;

	public URLExtractorModelLoader(@Inject(LmRuntimeBuilder.FILE_EXTRACTOR_MODEL_ROOT_DIR) URL rootUrl) {
		LOGGER.info("Extractor XML files will be located under '{}'", rootUrl);
		this.rootUrl = rootUrl;
	}

    @Override
    protected Reader getXmlSource(String name) throws IOException {

        if (!name.endsWith(".xml")) {
			name = name + ".xml";
		}

        URL modelUrl = new URL(rootUrl, name);

        LOGGER.info("Will extractor XML from {}", modelUrl);

		return new InputStreamReader(modelUrl.openStream(), "UTF-8");
    }

    @Override
    public boolean needsReload(ExtractorModelContainer container) {
        return false;
    }
}
