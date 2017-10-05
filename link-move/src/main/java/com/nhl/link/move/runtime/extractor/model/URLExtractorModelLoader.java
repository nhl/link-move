package com.nhl.link.move.runtime.extractor.model;

import com.nhl.link.move.extractor.model.ExtractorModelContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;

/**
 * @since 2.4
 */
public class URLExtractorModelLoader extends BaseExtractorModelLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(URLExtractorModelLoader.class);

    private URL baseUrl;

    public URLExtractorModelLoader(URL baseUrl) {
        LOGGER.info("Extractor XML files will be located under '{}'", baseUrl);
        this.baseUrl = baseUrl;
    }

    @Override
    protected Reader getXmlSource(String name) throws IOException {

        if (!name.endsWith(".xml")) {
            name = name + ".xml";
        }

        URL modelUrl = new URL(baseUrl, name);

        LOGGER.info("Will extract XML from {}", modelUrl);

        return new InputStreamReader(modelUrl.openStream(), "UTF-8");
    }

    @Override
    public boolean needsReload(ExtractorModelContainer container) {
        return false;
    }
}
