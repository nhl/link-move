package com.nhl.link.move.itest;

import com.nhl.link.move.Execution;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.unit.LmIntegrationTest;
import com.nhl.link.move.unit.LmTaskTester;
import com.nhl.link.move.unit.cayenne.t.Etl1t;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CreateIT extends LmIntegrationTest {

    @Test
    public void testSync() {

        LmTask task = etl.service(ITaskService.class).create(Etl1t.class)
                .sourceExtractor("com/nhl/link/move/itest/etl1_to_etl1t.xml").task();

        srcRunSql("INSERT INTO utest.etl1 (NAME, AGE) VALUES ('a', 3)");
        srcRunSql("INSERT INTO utest.etl1 (NAME, AGE) VALUES ('b', NULL)");

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

        srcRunSql("INSERT INTO utest.etl1 (NAME) VALUES ('c')");
        srcRunSql("UPDATE utest.etl1 SET AGE = 5 WHERE NAME = 'a'");

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

        srcRunSql("DELETE FROM utest.etl1 WHERE NAME = 'a'");

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
}
