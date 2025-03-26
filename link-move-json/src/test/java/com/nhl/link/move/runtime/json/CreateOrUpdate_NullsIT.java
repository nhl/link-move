package com.nhl.link.move.runtime.json;

import com.nhl.link.move.Execution;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.connect.StreamConnector;
import com.nhl.link.move.connect.URLConnector;
import com.nhl.link.move.json.unit.LmJsonIntegrationTest;
import com.nhl.link.move.json.unit.cayenne.t.Etlt1;
import com.nhl.link.move.runtime.LmRuntimeBuilder;
import org.junit.jupiter.api.Test;

import java.net.URL;

public class CreateOrUpdate_NullsIT extends LmJsonIntegrationTest {

    @Override
    protected LmRuntimeBuilder testRuntimeBuilder() {
        URL url = CreateOrUpdate_NullsIT.class.getClassLoader().getResource("com/nhl/link/move/json/src_json/etl1_src_nulls.json");
        return super.testRuntimeBuilder()
                .connector(StreamConnector.class, "etl_src_id", URLConnector.of(url));
    }

    @Test
    public void testCreateOrUpdate() {

        etlt1().insertColumns("id", "num_int", "string")
                .values(15, 12, null)
                .values(16, 1, "z")
                .exec();

        LmTask task = lmRuntime.getTaskService()
                .createOrUpdate(Etlt1.class)
                .sourceExtractor("com/nhl/link/move/json/extractor/etl1_src_to_etl1t.xml")
                .task();

        Execution e1 = task.run();
        assertExec(3, 2, 1, 0, e1);
        etlt1().matcher().assertMatches(4);
        etlt1().matcher().eq("id", 12).andEq("num_int", 24).andEq("string", "s").assertOneMatch();
        etlt1().matcher().eq("id", 15).andEq("num_int", null).andEq("string", null).assertOneMatch();
        etlt1().matcher().eq("id", -1).andEq("num_int", null).andEq("string", null).assertOneMatch();
    }
}
