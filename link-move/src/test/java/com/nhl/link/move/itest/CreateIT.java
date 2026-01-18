package com.nhl.link.move.itest;

import com.nhl.link.move.Execution;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.unit.LmIntegrationTest;
import com.nhl.link.move.unit.cayenne.t.Etl1t;
import com.nhl.link.move.unit.cayenne.t.Etl3t;
import org.junit.jupiter.api.Test;

public class CreateIT extends LmIntegrationTest {

    @Test
    public void sync_MultiBatch() {

        LmTask task = lmRuntime.service(ITaskService.class)
                .create(Etl1t.class)
                .batchSize(2)
                .sourceExtractor("com/nhl/link/move/itest/etl1_to_etl1t_upper.xml")
                .task();

        srcEtl1().insertColumns("name", "age")
                .values("a", 3)
                .values("b", null)
                .values("c", 6)
                .values("d", 8)
                .values("e", 12)
                .exec();

        Execution e1 = task.run();
        assertExec(5, 5, 0, 0, e1);
    }

    @Test
    public void sync() {

        LmTask task = lmRuntime.service(ITaskService.class)
                .create(Etl1t.class)
                .sourceExtractor("com/nhl/link/move/itest/etl1_to_etl1t_upper.xml")
                .task();

        srcEtl1().insertColumns("name", "age")
                .values("a", 3)
                .values("b", null)
                .exec();

        Execution e1 = task.run();
        assertExec(2, 2, 0, 0, e1);

        etl1t().matcher().assertMatches(2);
        etl1t().matcher().eq("name", "a").andEq("age", 3).assertOneMatch();
        etl1t().matcher().eq("name", "b").andEq("age", null).assertOneMatch();

        srcEtl1().insertColumns("name").values("c").exec();
        srcEtl1().update().set("age", 5).where("name", "a").exec();

        Execution e2 = task.run();
        assertExec(3, 3, 0, 0, e2);

        etl1t().matcher().assertMatches(5);
        etl1t().matcher().eq("name", "a").andEq("age", 5).assertOneMatch();
        etl1t().matcher().eq("name", "c").andEq("age", null).assertOneMatch();

        srcEtl1().delete().where("name", "a").exec();

        Execution e3 = task.run();
        assertExec(2, 2, 0, 0, e3);

        etl1t().matcher().assertMatches(7);
        etl1t().matcher().eq("name", "a").andEq("age", 5).assertOneMatch();

        Execution e4 = task.run();
        assertExec(2, 2, 0, 0, e4);
    }

    @Test
    public void syncFk() {

        LmTask task = lmRuntime.service(ITaskService.class).create(Etl3t.class)
                .sourceExtractor("com/nhl/link/move/itest/etl3_to_etl3t.xml").task();

        srcEtl2().insertColumns("id", "address", "name")
                .values(34, "Address1", "2Name1")
                .values(58, "Address2", "2Name2")
                .exec();

        srcEtl5().insertColumns("id", "name")
                .values(17, "5Name1")
                .values(11, "5Name2")
                .exec();

        srcEtl3().insertColumns("e2_id", "e5_id", "name", "phone_number")
                .values(58, 17, "3Name1", "3PHONE1")
                .values(34, 17, "3Name2", "3PHONE2")
                .exec();

        etl2t().insertColumns("id", "address", "name")
                .values(34, "Address1", "2Name1")
                .values(58, "Address2", "2Name2")
                .exec();

        etl5t().insertColumns("id", "name")
                .values(17, "5Name1")
                .values(11, "5Name2")
                .exec();

        Execution e1 = task.run();
        assertExec(2, 2, 0, 0, e1);

        etl3t().matcher().assertMatches(2);
        etl3t().matcher().eq("e2_id", 58).andEq("e5_id", 17).andEq("name", "3Name1").andEq("phone_number", "3PHONE1")
                .assertOneMatch();
        etl3t().matcher().eq("e2_id", 34).andEq("e5_id", 17).andEq("name", "3Name2").andEq("phone_number", "3PHONE2")
                .assertOneMatch();
    }
}
