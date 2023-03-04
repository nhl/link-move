package com.nhl.link.move.runtime.connect;

import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.connect.StreamConnector;
import com.nhl.link.move.connect.URIConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Optional;

/**
 * @since 1.4
 * @deprecated as the factory treats "connectorId" as a URI, and this pattern is strongly discouraged. Instead, the
 * "connectorId" should be a symbolic name resolvable outside LinkMove.
 */
@Deprecated(since = "3.0")
public class URIConnectorFactory implements IConnectorFactory<StreamConnector> {

    private static final Logger LOGGER = LoggerFactory.getLogger(URIConnectorFactory.class);

    @Override
    public Class<StreamConnector> getConnectorType() {
        return StreamConnector.class;
    }

    @Override
    public Optional<URIConnector> createConnector(String id) {
        LOGGER.warn("*** URIConnectorFactory is deprecated. Treating connector ID as a URL is discouraged. Connector ID: {}", id);

        URI uri = toUri(id);
        return Optional.ofNullable(uri)
                .map(this::checkAbsolute)
                .map(URIConnector::new);
    }

    private URI toUri(String id) {
        try {
            // first try to get resource from classpath
            URL url = URIConnectorFactory.class.getResource(id);
            if (url != null) {
                return url.toURI();
            } else {
                // probably we have absolute path to file here
                return new URI(id);
            }
        } catch (URISyntaxException e) {
            return null;
        }
    }

    private URI checkAbsolute(URI uri) {
        if (!uri.isAbsolute()) {
            throw new LmRuntimeException("Provided URI is not absolute (should be a classpath resource or absolute path to file)");
        }

        return uri;
    }
}
