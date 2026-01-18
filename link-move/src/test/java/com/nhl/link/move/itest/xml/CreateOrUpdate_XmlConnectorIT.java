package com.nhl.link.move.itest.xml;

import com.nhl.link.move.Execution;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.connect.StreamConnector;
import com.nhl.link.move.connect.URLConnector;
import com.nhl.link.move.runtime.LmRuntimeBuilder;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.unit.LmIntegrationTest;
import com.nhl.link.move.unit.cayenne.t.Etl1t;
import org.junit.jupiter.api.Test;

import java.net.URL;

public class CreateOrUpdate_XmlConnectorIT extends LmIntegrationTest {

    @Override
    protected LmRuntimeBuilder testRuntimeBuilder() {
        URL xml = getClass().getResource("f2.xml");
        return super.testRuntimeBuilder()
                .connector(StreamConnector.class, "f2.xml", URLConnector.of(xml));
    }

    @Test
    public void multiConnectors_XMLSource() {

        LmTask task = lmRuntime.service(ITaskService.class)
                .createOrUpdate(Etl1t.class)
                .sourceExtractor("com/nhl/link/move/itest/xml/xml_to_etl1t.xml")
                .matchBy(Etl1t.NAME)
                .task();

        Execution e1 = task.run();
        assertExec(1, 1, 0, 0, e1);

        etl1t().matcher().assertOneMatch();
        etl1t().matcher().eq("name", "zzz").andEq("age", 3).assertOneMatch();
    }
}
