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

    protected LmRuntime etl;

    @BeforeEach
    public void before() {
        this.etl = createEtl();
    }

    @AfterEach
    public void shutdown() {
        if (etl != null) {
            etl.shutdown();
        }
    }

    protected LmRuntime createEtl() {
        Connector c = new DataSourceConnector("derbysrc", srcDb.getDataSource());
        return new LmRuntimeBuilder()
                .withConnector("derbysrc", c)
                .withTargetRuntime(targetStack.runtime())
                .withConnectorFactory(StreamConnector.class, new URIConnectorFactory()).build();
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
