package com.nhl.link.etl.itest;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.nhl.link.etl.EtlTask;
import com.nhl.link.etl.Execution;
import com.nhl.link.etl.unit.EtlIntegrationTest;
import com.nhl.link.etl.unit.cayenne.t.Etl1t;

public class DeleteIT extends EtlIntegrationTest {

	@Test
	public void test_ByAttribute() {

		EtlTask task = etl.getTaskService().delete(Etl1t.class)
				.sourceMatchExtractor("com/nhl/link/etl/itest/etl1_to_etl1t").matchBy(Etl1t.NAME).task();

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

		EtlTask task = etl.getTaskService().delete(Etl1t.class).batchSize(2)
				.sourceMatchExtractor("com/nhl/link/etl/itest/etl1_to_etl1t").matchBy(Etl1t.NAME).task();

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
