package com.nhl.link.move.itest;

import com.nhl.link.move.Execution;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.unit.LmIntegrationTest;
import com.nhl.link.move.unit.cayenne.t.Etl1t;
import com.nhl.link.move.unit.cayenne.t.Etl6t;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DeleteIT extends LmIntegrationTest {

	@Test
	public void test_ById_Normalized_IntegerToLong() {

		LmTask task = etl.service(ITaskService.class).delete(Etl6t.class)
				.sourceMatchExtractor("com/nhl/link/move/itest/etl6_to_etl6t_byid.xml").task();

		targetRunSql("INSERT INTO utest.etl6t (ID, NAME) VALUES (1, 'a')");
		targetRunSql("INSERT INTO utest.etl6t (ID, NAME) VALUES (2, 'b')");

		srcRunSql("INSERT INTO utest.etl6 (ID, NAME) VALUES (1, 'a')");

		Execution e2 = task.run();
		assertExec(1, 0, 0, 1, e2);
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl6t WHERE NAME = 'a'"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl6t"));
	}

	@Test
	public void test_ByAttribute() {

		LmTask task = etl.service(ITaskService.class).delete(Etl1t.class)
				.sourceMatchExtractor("com/nhl/link/move/itest/etl1_to_etl1t.xml").matchBy(Etl1t.NAME).task();

		targetRunSql("INSERT INTO utest.etl1t (NAME, AGE) VALUES ('a', 3)");
		targetRunSql("INSERT INTO utest.etl1t (NAME, AGE) VALUES ('b', NULL)");

		Execution e1 = task.run();
		assertExec(0, 0, 0, 2, e1);

		assertEquals(0, targetScalar("SELECT count(1) from utest.etl1t"));

		targetRunSql("INSERT INTO utest.etl1t (NAME, AGE) VALUES ('a', 3)");
		targetRunSql("INSERT INTO utest.etl1t (NAME, AGE) VALUES ('b', NULL)");

		srcRunSql("INSERT INTO utest.etl1 (NAME) VALUES ('a')");

		Execution e2 = task.run();
		assertExec(1, 0, 0, 1, e2);
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl1t WHERE NAME = 'a'"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl1t"));
	}

	@Test
	public void test_ByAttribute_MultiBatch() {

		LmTask task = etl.service(ITaskService.class).delete(Etl1t.class).batchSize(2)
				.sourceMatchExtractor("com/nhl/link/move/itest/etl1_to_etl1t.xml").matchBy(Etl1t.NAME).task();

		srcRunSql("INSERT INTO utest.etl1 (NAME) VALUES ('a')");
		srcRunSql("INSERT INTO utest.etl1 (NAME) VALUES ('b')");
		srcRunSql("INSERT INTO utest.etl1 (NAME) VALUES ('c')");
		srcRunSql("INSERT INTO utest.etl1 (NAME) VALUES ('d')");
		srcRunSql("INSERT INTO utest.etl1 (NAME) VALUES ('e')");

		targetRunSql("INSERT INTO utest.etl1t (NAME, AGE) VALUES ('a', 3)");
		targetRunSql("INSERT INTO utest.etl1t (NAME, AGE) VALUES ('d', NULL)");
		targetRunSql("INSERT INTO utest.etl1t (NAME, AGE) VALUES ('f', NULL)");
		targetRunSql("INSERT INTO utest.etl1t (NAME, AGE) VALUES ('g', NULL)");
		targetRunSql("INSERT INTO utest.etl1t (NAME, AGE) VALUES ('h', NULL)");

		Execution e1 = task.run();
		assertExec(5, 0, 0, 3, e1);

		assertEquals(2, targetScalar("SELECT count(1) from utest.etl1t"));
	}
}
