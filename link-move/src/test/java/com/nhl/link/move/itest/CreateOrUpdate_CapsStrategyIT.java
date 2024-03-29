package com.nhl.link.move.itest;

import com.nhl.link.move.Execution;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.unit.LmIntegrationTest;
import com.nhl.link.move.unit.cayenne.t.Etl12t;
import com.nhl.link.move.unit.cayenne.t.Etl1t;
import org.junit.jupiter.api.Test;

public class CreateOrUpdate_CapsStrategyIT extends LmIntegrationTest {

    @Test
    public void test_CapsLower() {

        LmTask task = lmRuntime.service(ITaskService.class)
                .createOrUpdate(Etl1t.class)
                .sourceExtractor("com/nhl/link/move/itest/etl1_to_etl1t_lower.xml")
                .matchBy(Etl1t.NAME)
                .task();

        srcEtl1().insertColumns("name", "age")
                .values("a", 3)
                .values("b", null)
                .exec();

        Execution e1 = task.run();
        assertExec(2, 2, 0, 0, e1);
        etl1t().matcher().assertMatches(2);
        etl1t().matcher().eq("name", "a").eq("age", 3).assertOneMatch();
        etl1t().matcher().eq("name", "b").eq("age", null).assertOneMatch();
    }

    @Test
    public void test_DefaultCaps_MixedCase() {

        LmTask task = lmRuntime.service(ITaskService.class).
                createOrUpdate(Etl12t.class)
                .sourceExtractor("com/nhl/link/move/itest/etl12_to_etl12t_default_caps.xml")
                .task();

        srcEtl12().insertColumns("MixedCaseId", "StartsWithUpperCase").values(1, "a").values(2, "b").exec();
        Execution e1 = task.run();
        assertExec(2, 2, 0, 0, e1);

        srcEtl12().update().set("StartsWithUpperCase", 5).where("MixedCaseId", 2).exec();
        Execution e2 = task.run();
        assertExec(2, 0, 1, 0, e2);
    }
}
