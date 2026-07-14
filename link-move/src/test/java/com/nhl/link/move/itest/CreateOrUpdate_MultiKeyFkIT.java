package com.nhl.link.move.itest;

import com.nhl.link.move.Execution;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.unit.LmIntegrationTest;
import com.nhl.link.move.unit.cayenne.t.Etl14t;
import org.junit.jupiter.api.Test;

/**
 * Verifies syncing a to-one relationship backed by a multi-column (composite) FK. The source
 * provides both FK columns directly, and they match the composite PK of the already-loaded
 * parent. Mirrors the "nhl_playoff_series_game" -> "nhl_playoff_series" case.
 */
public class CreateOrUpdate_MultiKeyFkIT extends LmIntegrationTest {

    @Test
    public void syncMultiColumnFk() {

        LmTask task = lmRuntime.service(ITaskService.class)
                .createOrUpdate(Etl14t.class)
                .sourceExtractor("com/nhl/link/move/itest/etl14_to_etl14t.xml")
                .matchById()
                .task();

        // parent (composite PK) is already in the target DB
        etl13t().insertColumns("k1", "k2", "name")
                .values(1, "A", "series-1A")
                .values(1, "B", "series-1B")
                .values(2, "A", "series-2A")
                .exec();

        srcEtl14().insertColumns("id", "k1", "k2", "name")
                .values(10, 1, "A", "game-10")
                .values(11, 1, "B", "game-11")
                .values(12, 2, "A", "game-12")
                .exec();

        Execution e1 = task.run();
        assertExec(3, 3, 0, 0, e1);

        etl14t().matcher().assertMatches(3);
        etl14t().matcher().eq("id", 10).andEq("k1", 1).andEq("k2", "A").assertOneMatch();
        etl14t().matcher().eq("id", 11).andEq("k1", 1).andEq("k2", "B").assertOneMatch();
        etl14t().matcher().eq("id", 12).andEq("k1", 2).andEq("k2", "A").assertOneMatch();

        // re-point game 10 at a different parent series (composite FK update)
        srcEtl14().update().set("k2", "B").where("id", 10).exec();

        Execution e2 = task.run();
        assertExec(3, 0, 1, 0, e2);

        etl14t().matcher().assertMatches(3);
        etl14t().matcher().eq("id", 10).andEq("k1", 1).andEq("k2", "B").assertOneMatch();
    }
}
