package com.nhl.link.move.unit;

import com.nhl.link.move.Execution;
import com.nhl.link.move.connect.Connector;
import com.nhl.link.move.connect.StreamConnector;
import com.nhl.link.move.runtime.LmRuntime;
import com.nhl.link.move.runtime.LmRuntimeBuilder;
import com.nhl.link.move.runtime.connect.URIConnectorFactory;
import com.nhl.link.move.runtime.jdbc.DataSourceConnector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public abstract class LmIntegrationTest extends DerbySrcTargetTest {

    protected LmRuntime lmRuntime;

    @BeforeEach
    protected void initLmRuntime() {
        this.lmRuntime = testRuntimeBuilder().build();
    }

    @AfterEach
    protected void stopLmRuntime() {
        if (lmRuntime != null) {
            lmRuntime.shutdown();
        }
    }

    protected LmRuntimeBuilder testRuntimeBuilder() {
        Connector c = new DataSourceConnector("derbysrc", srcDb.getDataSource());
        return new LmRuntimeBuilder()
                .withTargetRuntime(targetCayenne.getRuntime())
                .withConnector("derbysrc", c)
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
