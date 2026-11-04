package com.nhl.link.move.itest.runtime.jdbc;

import com.nhl.link.move.runtime.jdbc.DataSourceConnector;
import com.nhl.link.move.unit.DerbySrcTest;
import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.query.SQLExec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class DataSourceConnectorIT extends DerbySrcTest {

    private DataSourceConnector connector;

    @BeforeEach
    public void startConnector() {
        this.connector = new DataSourceConnector(srcDb.getDataSource());
    }

    @AfterEach
    public void stopConnector() {
        connector.shutdown();
    }

    @Test
    public void sharedContext() {
        ObjectContext context = connector.sharedContext();
        assertNotNull(context);

        SQLExec.query("INSERT INTO \"etl1\" (\"name\") VALUES ('a')").execute(context);
        srcDb.getTable("etl1").matcher().assertOneMatch();
    }

}
