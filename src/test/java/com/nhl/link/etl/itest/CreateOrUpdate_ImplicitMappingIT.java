package com.nhl.link.etl.itest;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.nhl.link.etl.EtlTask;
import com.nhl.link.etl.Execution;
import com.nhl.link.etl.unit.EtlIntegrationTest;
import com.nhl.link.etl.unit.cayenne.t.Etl1t;
import com.nhl.link.etl.unit.cayenne.t.Etl5t;

public class CreateOrUpdate_ImplicitMappingIT extends EtlIntegrationTest {

	@Test
	public void test_ByDbAttribute() {

		EtlTask task = etl.getTaskService().createOrUpdate(Etl1t.class)
				.sourceExtractor("com/nhl/link/etl/itest/etl1_to_etl1t_implicit").matchBy("db:name").task();

		srcRunSql("INSERT INTO utest.etl1 (NAME, AGE) VALUES ('a', 3)");
		srcRunSql("INSERT INTO utest.etl1 (NAME, AGE) VALUES ('b', NULL)");

		Execution e1 = task.run();
		assertExec(2, 2, 0, 0, e1);
		assertEquals(2, targetScalar("SELECT count(1) from utest.etl1t"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl1t WHERE NAME = 'a' AND age = 3"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl1t WHERE NAME = 'b' AND age is null"));

		srcRunSql("INSERT INTO utest.etl1 (NAME) VALUES ('c')");
		srcRunSql("UPDATE utest.etl1 SET AGE = 5 WHERE NAME = 'a'");

		Execution e2 = task.run();
		assertExec(3, 1, 1, 0, e2);
		assertEquals(3, targetScalar("SELECT count(1) from utest.etl1t"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl1t WHERE NAME = 'a' AND age = 5"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl1t WHERE NAME = 'c' AND age is null"));

		srcRunSql("DELETE FROM utest.etl1 WHERE NAME = 'a'");

		Execution e3 = task.run();
		assertExec(2, 0, 0, 0, e3);
		assertEquals(3, targetScalar("SELECT count(1) from utest.etl1t"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl1t WHERE NAME = 'a' AND age = 5"));

		Execution e4 = task.run();
		assertExec(2, 0, 0, 0, e4);
	}

	@Test
	public void test_ById() {

		EtlTask task = etl.getTaskService().createOrUpdate(Etl5t.class)
				.sourceExtractor("com/nhl/link/etl/itest/etl5_to_etl5t_byid_implicit.xml").matchById().task();

		srcRunSql("INSERT INTO utest.etl5 (ID, NAME) VALUES (45, 'a')");
		srcRunSql("INSERT INTO utest.etl5 (ID, NAME) VALUES (11, 'b')");

		Execution e1 = task.run();
		assertExec(2, 2, 0, 0, e1);
		assertEquals(2, targetScalar("SELECT count(1) from utest.etl5t"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl5t WHERE NAME = 'a' AND ID = 45"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl5t WHERE NAME = 'b' AND ID = 11"));

		srcRunSql("INSERT INTO utest.etl5 (ID, NAME) VALUES (31, 'c')");
		srcRunSql("UPDATE utest.etl5 SET NAME = 'd' WHERE ID = 45");

		Execution e2 = task.run();
		assertExec(3, 1, 1, 0, e2);
		assertEquals(3, targetScalar("SELECT count(1) from utest.etl5t"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl5t WHERE NAME = 'd' AND ID = 45"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl5t WHERE NAME = 'c' AND ID = 31"));

		srcRunSql("DELETE FROM utest.etl5 WHERE ID = 45");

		Execution e3 = task.run();
		assertExec(2, 0, 0, 0, e3);
		assertEquals(3, targetScalar("SELECT count(1) from utest.etl5t"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl5t WHERE NAME = 'd' AND ID = 45"));

		Execution e4 = task.run();
		assertExec(2, 0, 0, 0, e4);
	}
}
