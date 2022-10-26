package com.nhl.link.move.runtime.json;

import com.nhl.link.move.Execution;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.json.unit.LmJsonIntegrationTest;
import com.nhl.link.move.json.unit.cayenne.t.Etlt1;
import com.nhl.link.move.runtime.LmRuntime;
import org.junit.jupiter.api.Test;

public class CreateOrUpdateIT extends LmJsonIntegrationTest {

    @Test
    public void testCreate() {

        LmRuntime runtime = testRuntimeBuilder("etl_src_id", "com/nhl/link/move/json/src_json/etl1_src.json").build();
        LmTask task = runtime.getTaskService()
                .createOrUpdate(Etlt1.class)
                .sourceExtractor("com/nhl/link/move/json/extractor/etl1_src_to_etl1t.xml")
                .task();

        Execution e1 = task.run();
        assertExec(3, 3, 0, 0, e1);
        etlt1().matcher().assertMatches(3);
        etlt1().matcher().eq("id", 12).eq("num_int", 24).eq("string", "s").assertOneMatch();
        etlt1().matcher().eq("id", 15).eq("num_int", 11).eq("string", "x").assertOneMatch();
        etlt1().matcher().eq("id", -1).eq("num_int", 4).eq("string", "a").assertOneMatch();
    }

    @Test
    public void testCreateOrUpdate() {

        etlt1().insertColumns("id", "num_int", "string")
                .values(15, 12, null)
                .values(16, 1, "z")
                .exec();

        LmRuntime runtime = testRuntimeBuilder("etl_src_id", "com/nhl/link/move/json/src_json/etl1_src.json").build();
        LmTask task = runtime.getTaskService()
                .createOrUpdate(Etlt1.class)
                .sourceExtractor("com/nhl/link/move/json/extractor/etl1_src_to_etl1t.xml")
                .task();

        Execution e1 = task.run();
        assertExec(3, 2, 1, 0, e1);
        etlt1().matcher().assertMatches(4);
        etlt1().matcher().eq("id", 12).eq("num_int", 24).eq("string", "s").assertOneMatch();
        etlt1().matcher().eq("id", 15).eq("num_int", 11).eq("string", "x").assertOneMatch();
        etlt1().matcher().eq("id", -1).eq("num_int", 4).eq("string", "a").assertOneMatch();
    }
}
