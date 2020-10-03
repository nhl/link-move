package com.nhl.link.move.itest;

import com.nhl.link.move.Execution;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.unit.LmIntegrationTest;
import com.nhl.link.move.unit.cayenne.ti.TiSub1;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
        assertEquals(2, targetScalar("SELECT count(1) from \"ti_super\""));
        assertEquals(2, targetScalar("SELECT count(1) from \"ti_super\" WHERE \"type\" = 'sub1'"));

        assertEquals(2, targetScalar("SELECT count(1) from \"ti_sub1\""));
        assertEquals(1, targetScalar("SELECT count(1) from \"ti_sub1\" WHERE \"sub_key\" = 'a' AND \"subp1\" = 'p1'"));
        assertEquals(1, targetScalar("SELECT count(1) from \"ti_sub1\" WHERE \"sub_key\" = 'b' AND \"subp1\" = 'p2'"));

        srcEtlSub1().insertColumns("s_key", "s_subp1").values("c", null).exec();
        targetRunSql("UPDATE \"ti_sub1\" SET \"subp1\" = 'p3' WHERE \"subp1\" = 'p1'");

        Execution e2 = task.run();
        assertExec(3, 1, 1, 0, e2);
        assertEquals(3, targetScalar("SELECT count(1) from \"ti_super\""));
        assertEquals(3, targetScalar("SELECT count(1) from \"ti_super\" WHERE \"type\" = 'sub1'"));

        assertEquals(3, targetScalar("SELECT count(1) from \"ti_sub1\""));
        assertEquals(1, targetScalar("SELECT count(1) from \"ti_sub1\" WHERE \"sub_key\" = 'a' AND \"subp1\" = 'p1'"));
        assertEquals(1, targetScalar("SELECT count(1) from \"ti_sub1\" WHERE \"sub_key\" = 'b' AND \"subp1\" = 'p2'"));
        assertEquals(1, targetScalar("SELECT count(1) from \"ti_sub1\" WHERE \"sub_key\" = 'c' AND \"subp1\" IS NULL"));

        srcEtlSub1().delete().and("s_key", "a").exec();

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
        assertEquals(2, targetScalar("SELECT count(1) from \"ti_super\""));
        assertEquals(1, targetScalar("SELECT count(1) from \"ti_super\" WHERE \"type\" = 'sub1' AND \"super_key\" = 'a'"));
        assertEquals(1, targetScalar("SELECT count(1) from \"ti_super\" WHERE \"type\" = 'sub1' AND \"super_key\" = 'b'"));

        assertEquals(2, targetScalar("SELECT count(1) from \"ti_sub1\""));
        assertEquals(1, targetScalar("SELECT count(1) from \"ti_sub1\" WHERE \"sub_key\" is null AND \"subp1\" = 'p1'"));
        assertEquals(1, targetScalar("SELECT count(1) from \"ti_sub1\" WHERE \"sub_key\" is null AND \"subp1\" = 'p2'"));

        srcEtlSub1().insertColumns("s_key", "s_subp1").values("c", null).exec();
        targetRunSql("UPDATE \"ti_sub1\" SET \"subp1\" = 'p3' WHERE \"subp1\" = 'p1'");

        Execution e2 = task.run();
        assertExec(3, 1, 1, 0, e2);
        assertEquals(3, targetScalar("SELECT count(1) from \"ti_super\""));
        assertEquals(1, targetScalar("SELECT count(1) from \"ti_super\" WHERE \"type\" = 'sub1' AND \"super_key\" = 'a'"));
        assertEquals(1, targetScalar("SELECT count(1) from \"ti_super\" WHERE \"type\" = 'sub1' AND \"super_key\" = 'b'"));
        assertEquals(1, targetScalar("SELECT count(1) from \"ti_super\" WHERE \"type\" = 'sub1' AND \"super_key\" = 'c'"));

        assertEquals(3, targetScalar("SELECT count(1) from \"ti_sub1\""));
        assertEquals(1, targetScalar("SELECT count(1) from \"ti_sub1\" WHERE \"sub_key\" is null AND \"subp1\" = 'p1'"));
        assertEquals(1, targetScalar("SELECT count(1) from \"ti_sub1\" WHERE \"sub_key\" is null AND \"subp1\" = 'p2'"));
        assertEquals(1, targetScalar("SELECT count(1) from \"ti_sub1\" WHERE \"sub_key\" is null AND \"subp1\" is null"));

        srcEtlSub1().delete().and("s_key", "a").exec();

        Execution e3 = task.run();
        assertExec(2, 0, 0, 0, e3);

        Execution e4 = task.run();
        assertExec(2, 0, 0, 0, e4);
    }

}
