package com.nhl.link.move.json.unit;

import com.nhl.link.move.Execution;
import com.nhl.link.move.connect.StreamConnector;
import com.nhl.link.move.connect.URIConnector;
import com.nhl.link.move.runtime.LmRuntimeBuilder;
import com.nhl.link.move.runtime.connect.URIConnectorFactory;

import java.net.URI;
import java.net.URISyntaxException;

public abstract class LmJsonIntegrationTest extends DerbyTargetTest {

    protected LmRuntimeBuilder testRuntimeBuilder(String connectorId, String jsonUrl) {

        URI uri;
        try {
            uri = getClass().getClassLoader().getResource(jsonUrl).toURI();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        return new LmRuntimeBuilder()
                .withTargetRuntime(targetCayenne.getRuntime())
                .withConnector(connectorId, new URIConnector(uri))
                .withConnectorFactory(StreamConnector.class, new URIConnectorFactory());
    }

    protected void assertExec(int extracted, int created, int updated, int deleted, Execution exec) {
        new LmTaskTester()
                .shouldExtract(extracted)
                .shouldCreate(created)
                .shouldUpdate(updated)
                .shouldDelete(deleted)
                .test(exec);
    }
}
