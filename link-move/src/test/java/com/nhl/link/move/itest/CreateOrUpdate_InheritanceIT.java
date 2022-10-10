package com.nhl.link.move.itest;

import com.nhl.link.move.Execution;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.unit.LmIntegrationTest;
import com.nhl.link.move.unit.cayenne.ti.TiSub1;
import org.junit.jupiter.api.Test;

public class CreateOrUpdate_InheritanceIT extends LmIntegrationTest {

    @Test
    public void test_Subclass_MatchBySubKey() {

        LmTask task = lmRuntime.service(ITaskService.class)
                .createOrUpdate(TiSub1.class)
                .sourceExtractor("com/nhl/link/move/itest/etl1_sub1_to_ti_sub1_sub_key.xml")
                .matchBy(TiSub1.SUB_KEY)
                .task();

        srcEtlSub1().insertColumns("s_key", "s_subp1")
                .values("a", "p1")
                .values("b", "p2")
                .exec();

        Execution e1 = task.run();
        assertExec(2, 2, 0, 0, e1);
        tiSuper().matcher().assertMatches(2);
        tiSuper().matcher().eq("type", "sub1").assertMatches(2);

        tiSub1().matcher().assertMatches(2);
        tiSub1().matcher().eq("sub_key", "a").eq("subp1", "p1").assertOneMatch();
        tiSub1().matcher().eq("sub_key", "b").eq("subp1", "p2").assertOneMatch();

        srcEtlSub1().insertColumns("s_key", "s_subp1").values("c", null).exec();
        tiSub1().update().set("subp1", "p3").where("subp1", "p1").exec();

        Execution e2 = task.run();
        assertExec(3, 1, 1, 0, e2);
        tiSuper().matcher().assertMatches(3);
        tiSuper().matcher().eq("type", "sub1").assertMatches(3);

        tiSub1().matcher().assertMatches(3);
        tiSub1().matcher().eq("sub_key", "a").eq("subp1", "p1").assertOneMatch();
        tiSub1().matcher().eq("sub_key", "b").eq("subp1", "p2").assertOneMatch();
        tiSub1().matcher().eq("sub_key", "c").eq("subp1", null).assertOneMatch();

        srcEtlSub1().delete().where("s_key", "a").exec();

        Execution e3 = task.run();
        assertExec(2, 0, 0, 0, e3);

        Execution e4 = task.run();
        assertExec(2, 0, 0, 0, e4);
    }

    @Test
    public void test_Subclass_MatchBySuperKey() {

        LmTask task = lmRuntime.service(ITaskService.class)
                .createOrUpdate(TiSub1.class)
                .sourceExtractor("com/nhl/link/move/itest/etl1_sub1_to_ti_sub1_super_key.xml")
                .matchBy(TiSub1.SUPER_KEY)
                .task();

        srcEtlSub1().insertColumns("s_key", "s_subp1")
                .values("a", "p1")
                .values("b", "p2")
                .exec();

        Execution e1 = task.run();
        assertExec(2, 2, 0, 0, e1);
        tiSuper().matcher().assertMatches(2);
        tiSuper().matcher().eq("type", "sub1").eq("super_key", "a").assertOneMatch();
        tiSuper().matcher().eq("type", "sub1").eq("super_key", "b").assertOneMatch();

        tiSub1().matcher().assertMatches(2);
        tiSub1().matcher().eq("sub_key", null).eq("subp1", "p1").assertOneMatch();
        tiSub1().matcher().eq("sub_key", null).eq("subp1", "p2").assertOneMatch();

        srcEtlSub1().insertColumns("s_key", "s_subp1").values("c", null).exec();
        tiSub1().update().set("subp1", "p3").where("subp1", "p1").exec();

        Execution e2 = task.run();
        assertExec(3, 1, 1, 0, e2);

        tiSuper().matcher().assertMatches(3);
        tiSuper().matcher().eq("type", "sub1").eq("super_key", "a").assertOneMatch();
        tiSuper().matcher().eq("type", "sub1").eq("super_key", "b").assertOneMatch();
        tiSuper().matcher().eq("type", "sub1").eq("super_key", "c").assertOneMatch();

        tiSub1().matcher().assertMatches(2);
        tiSub1().matcher().eq("sub_key", null).eq("subp1", "p1").assertOneMatch();
        tiSub1().matcher().eq("sub_key", null).eq("subp1", "p2").assertOneMatch();

        srcEtlSub1().delete().where("s_key", "a").exec();

        Execution e3 = task.run();
        assertExec(2, 0, 0, 0, e3);

        Execution e4 = task.run();
        assertExec(2, 0, 0, 0, e4);
    }

}
