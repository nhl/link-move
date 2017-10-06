package com.nhl.link.move.resource;

import com.nhl.link.move.LmRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * @since 2.4
 */
public class URLResourceResolver implements ResourceResolver {

    private static final Logger LOGGER = LoggerFactory.getLogger(URLResourceResolver.class);

    private URL baseUrl;

    public URLResourceResolver(URL baseUrl) {
        LOGGER.info("Extractor XML files will be located under '{}'", baseUrl);
        this.baseUrl = baseUrl;
    }

    @Override
    public Reader reader(String name) {

        if (!name.endsWith(".xml")) {
            complainOfExtension(name);
            name = name + ".xml";
        }

        URL modelUrl;
        try {
            modelUrl = new URL(baseUrl, name);
        } catch (MalformedURLException e) {
            throw new LmRuntimeException("Error building source URL", e);
        }

        LOGGER.info("Will extract XML from {}", modelUrl);

        try {
            return new InputStreamReader(modelUrl.openStream(), "UTF-8");
        } catch (IOException e) {
            throw new LmRuntimeException("Error reading extractor config XML from URL " + modelUrl, e);
        }
    }

    /**
     * @param name
     * @deprecated since 2.4
     */
    @Deprecated
    private void complainOfExtension(String name) {
        LOGGER.warn("*** Implicit extension name is deprecated. Use '{}.xml' instead of '{}'", name, name);
    }
}
