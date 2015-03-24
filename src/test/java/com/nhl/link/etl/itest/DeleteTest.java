package com.nhl.link.etl.itest;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.nhl.link.etl.EtlTask;
import com.nhl.link.etl.Execution;
import com.nhl.link.etl.unit.EtlIntegrationTest;
import com.nhl.link.etl.unit.cayenne.t.Etl1t;

public class DeleteTest extends EtlIntegrationTest {

	@Test
	public void test_ByAttribute() {

		EtlTask task = etl.getTaskService().delete(Etl1t.class)
				.sourceMatchExtractor("com/nhl/link/etl/itest/etl1_to_etl1t").matchBy(Etl1t.NAME).task();

		targetRunSql("INSERT INTO utest.etl1t (NAME, AGE) VALUES ('a', 3)");
		targetRunSql("INSERT INTO utest.etl1t (NAME, AGE) VALUES ('b', NULL)");

		Execution e1 = task.run();

		assertEquals(0, targetScalar("SELECT count(1) from utest.etl1t"));

		targetRunSql("INSERT INTO utest.etl1t (NAME, AGE) VALUES ('a', 3)");
		targetRunSql("INSERT INTO utest.etl1t (NAME, AGE) VALUES ('b', NULL)");

		srcRunSql("INSERT INTO utest.etl1 (NAME) VALUES ('a')");

		Execution e2 = task.run();
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl1t WHERE NAME = 'a'"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl1t"));

	}
}
