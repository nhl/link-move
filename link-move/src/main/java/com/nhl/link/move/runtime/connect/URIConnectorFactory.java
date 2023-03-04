package com.nhl.link.move.runtime.connect;

import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.connect.StreamConnector;
import com.nhl.link.move.connect.URIConnector;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Optional;

/**
 * @since 1.4
 */
// TODO: deprecate this factory. We should not treat connector IDs as meaningful URLs
public class URIConnectorFactory implements IConnectorFactory<StreamConnector> {

    @Override
    public Class<StreamConnector> getConnectorType() {
        return StreamConnector.class;
    }

    @Override
    public Optional<URIConnector> createConnector(String id) {
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
