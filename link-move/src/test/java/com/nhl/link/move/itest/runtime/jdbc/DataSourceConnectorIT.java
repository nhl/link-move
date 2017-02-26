package com.nhl.link.move.itest.runtime.jdbc;

import com.nhl.link.move.runtime.jdbc.DataSourceConnector;
import com.nhl.link.move.unit.DerbySrcTest;
import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.access.DataDomain;
import org.apache.cayenne.access.types.ExtendedType;
import org.apache.cayenne.dba.DbAdapter;
import org.apache.cayenne.java8.access.types.LocalDateTimeType;
import org.apache.cayenne.query.SQLExec;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.time.LocalDateTime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DataSourceConnectorIT extends DerbySrcTest {

    private DataSourceConnector connector;

    @Before
    public void setUp() throws SQLException {
        this.connector = new DataSourceConnector(srcDataSource);
    }

    @After
    public void tearDown() throws SQLException {
        connector.shutdown();
    }

    @Test
    public void testSharedContext() {
        ObjectContext context = connector.sharedContext();
        assertNotNull(context);

        SQLExec.query("INSERT INTO utest.etl1 (NAME) VALUES ('a')").execute(context);
        assertEquals(1, srcScalar("SELECT count(1) from utest.etl1"));
    }

    @Test
    public void testCayenneJava8Support() {
        ObjectContext context = connector.sharedContext();

        DataDomain domain = (DataDomain) context.getChannel();
        DbAdapter adapter = domain.getDataNodes().iterator().next().getAdapter();
        ExtendedType java8OnlyType = adapter.getExtendedTypes().getRegisteredType(LocalDateTime.class);
        assertTrue(java8OnlyType instanceof LocalDateTimeType);
    }

}
