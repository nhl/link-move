package com.nhl.link.move.itest.runtime.jdbc;

import com.nhl.link.move.runtime.jdbc.DataSourceConnector;
import com.nhl.link.move.unit.DerbySrcTest;
import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.access.types.ValueObjectType;
import org.apache.cayenne.access.types.ValueObjectTypeRegistry;
import org.apache.cayenne.java8.access.types.LocalDateTimeValueType;
import org.apache.cayenne.query.SQLExec;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.time.LocalDateTime;

import static org.junit.Assert.*;

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

        ValueObjectTypeRegistry typeRegistry = context.getEntityResolver().getValueObjectTypeRegistry();
        ValueObjectType<LocalDateTime, ?> vt = typeRegistry.getValueType(LocalDateTime.class);
        assertTrue(vt instanceof LocalDateTimeValueType);
    }

}
