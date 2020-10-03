package com.nhl.link.move.itest;

import com.nhl.link.move.Execution;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.unit.LmIntegrationTest;
import com.nhl.link.move.unit.LmTaskTester;
import com.nhl.link.move.unit.cayenne.t.Etl1t;
import com.nhl.link.move.unit.cayenne.t.Etl3t;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CreateIT extends LmIntegrationTest {

    @Test
    public void testSync() {

        LmTask task = etl.service(ITaskService.class).create(Etl1t.class)
                .sourceExtractor("com/nhl/link/move/itest/etl1_to_etl1t_upper.xml").task();

        srcEtl1().insertColumns("name", "age")
                .values("a", 3)
                .values("b", null)
                .exec();

        Execution e1 = task.run();
        new LmTaskTester()
                .shouldExtract(2)
                .shouldCreate(2)
                .shouldUpdate(0)
                .shouldDelete(0)
                .test(e1);

        assertEquals(2, targetScalar("SELECT count(1) from utest.etl1t"));
        assertEquals(1, targetScalar("SELECT count(1) from utest.etl1t WHERE NAME = 'a' AND age = 3"));
        assertEquals(1, targetScalar("SELECT count(1) from utest.etl1t WHERE NAME = 'b' AND age is null"));

        srcEtl1().insertColumns("name").values("c").exec();
        srcEtl1().update().set("age", 5).where("name", "a").exec();

        Execution e2 = task.run();
        new LmTaskTester()
                .shouldExtract(3)
                .shouldCreate(3)
                .shouldUpdate(0)
                .shouldDelete(0)
                .test(e2);

        assertEquals(5, targetScalar("SELECT count(1) from utest.etl1t"));
        assertEquals(1, targetScalar("SELECT count(1) from utest.etl1t WHERE NAME = 'a' AND age = 5"));
        assertEquals(1, targetScalar("SELECT count(1) from utest.etl1t WHERE NAME = 'c' AND age is null"));

        srcEtl1().delete().and("name", "a").exec();

        Execution e3 = task.run();
        new LmTaskTester()
                .shouldExtract(2)
                .shouldCreate(2)
                .shouldUpdate(0)
                .shouldDelete(0)
                .test(e3);

        assertEquals(7, targetScalar("SELECT count(1) from utest.etl1t"));
        assertEquals(1, targetScalar("SELECT count(1) from utest.etl1t WHERE NAME = 'a' AND age = 5"));

        Execution e4 = task.run();
        new LmTaskTester()
                .shouldExtract(2)
                .shouldCreate(2)
                .shouldUpdate(0)
                .shouldDelete(0)
                .test(e4);
    }

    @Test
    public void test_SyncFk() {

        LmTask task = etl.service(ITaskService.class).create(Etl3t.class)
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

        targetRunSql("INSERT INTO utest.etl2t (ID, ADDRESS, NAME) VALUES (34, 'Address1', '2Name1')");
        targetRunSql("INSERT INTO utest.etl2t (ID, ADDRESS, NAME) VALUES (58, 'Address2', '2Name2')");
        targetRunSql("INSERT INTO utest.etl5t (ID, NAME) VALUES (17, '5Name1')");
        targetRunSql("INSERT INTO utest.etl5t (ID, NAME) VALUES (11, '5Name2')");

        Execution e1 = task.run();
        assertExec(2, 2, 0, 0, e1);
        assertEquals(2, targetScalar("SELECT count(1) from utest.etl3t"));
        assertEquals(1, targetScalar("SELECT count(1) from utest.etl3t "
                + "WHERE E2_ID = 58 AND E5_ID = 17 AND NAME = '3Name1' AND phone_number = '3PHONE1'"));
        assertEquals(1, targetScalar("SELECT count(1) from utest.etl3t "
                + "WHERE E2_ID = 34 AND E5_ID = 17 AND NAME = '3Name2' AND phone_number = '3PHONE2'"));
    }
}
