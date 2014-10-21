package com.nhl.link.etl.itest;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.nhl.link.etl.EtlRuntimeException;
import com.nhl.link.etl.EtlTask;
import com.nhl.link.etl.Execution;
import com.nhl.link.etl.SyncToken;
import com.nhl.link.etl.unit.EtlIntegrationTest;
import com.nhl.link.etl.unit.cayenne.t.Etl1t;
import com.nhl.link.etl.unit.cayenne.t.Etl5t;

public class CreateOrUpdateTest extends EtlIntegrationTest {

	@Test
	public void test_ByAttribute() {

		EtlTask task = etl.getTaskService().createTaskBuilder(Etl1t.class)
				.withExtractor("com/nhl/link/etl/itest/etl1_to_etl1t").matchBy(Etl1t.NAME).task();

		srcRunSql("INSERT INTO utest.etl1 (NAME, AGE) VALUES ('a', 3)");
		srcRunSql("INSERT INTO utest.etl1 (NAME, AGE) VALUES ('b', NULL)");

		Execution e1 = task.run();
		assertExec(2, 2, 0, e1);
		assertEquals(2, targetScalar("SELECT count(1) from utest.etl1t"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl1t WHERE NAME = 'a' AND age = 3"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl1t WHERE NAME = 'b' AND age is null"));

		srcRunSql("INSERT INTO utest.etl1 (NAME) VALUES ('c')");
		srcRunSql("UPDATE utest.etl1 SET AGE = 5 WHERE NAME = 'a'");

		Execution e2 = task.run();
		assertExec(3, 1, 1, e2);
		assertEquals(3, targetScalar("SELECT count(1) from utest.etl1t"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl1t WHERE NAME = 'a' AND age = 5"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl1t WHERE NAME = 'c' AND age is null"));

		srcRunSql("DELETE FROM utest.etl1 WHERE NAME = 'a'");

		Execution e3 = task.run();
		assertExec(2, 0, 0, e3);
		assertEquals(3, targetScalar("SELECT count(1) from utest.etl1t"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl1t WHERE NAME = 'a' AND age = 5"));

		Execution e4 = task.run();
		assertExec(2, 0, 0, e4);
	}

	@Test
	public void test_ByAttributes() {

		EtlTask task = etl.getTaskService().createTaskBuilder(Etl1t.class)
				.withExtractor("com/nhl/link/etl/itest/etl1_to_etl1t").matchBy(Etl1t.NAME, Etl1t.AGE).task();

		srcRunSql("INSERT INTO utest.etl1 (NAME, AGE) VALUES ('a', 3)");
		srcRunSql("INSERT INTO utest.etl1 (NAME, AGE) VALUES ('b', 5)");

		Execution e1 = task.run();
		assertExec(2, 2, 0, e1);
		assertEquals(2, targetScalar("SELECT count(1) from utest.etl1t"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl1t WHERE NAME = 'a' AND age = 3"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl1t WHERE NAME = 'b' AND age = 5"));

		// changing one of the key components should result in no-match and a
		// new record insertion
		srcRunSql("UPDATE utest.etl1 SET NAME = 'c' WHERE NAME = 'a'");

		Execution e2 = task.run();
		assertExec(2, 1, 0, e2);
		assertEquals(3, targetScalar("SELECT count(1) from utest.etl1t"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl1t WHERE NAME = 'c' AND age = 3"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl1t WHERE NAME = 'b' AND age = 5"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl1t WHERE NAME = 'a' AND age = 3"));

		Execution e4 = task.run();
		assertExec(2, 0, 0, e4);
	}

	@Test
	public void test_ById() {

		EtlTask task = etl.getTaskService().createTaskBuilder(Etl5t.class)
				.withExtractor("com/nhl/link/etl/itest/etl5_to_etl5t_byid.xml").matchById(Etl5t.ID_PK_COLUMN).task();

		srcRunSql("INSERT INTO utest.etl5 (ID, NAME) VALUES (45, 'a')");
		srcRunSql("INSERT INTO utest.etl5 (ID, NAME) VALUES (11, 'b')");

		Execution e1 = task.run();
		assertExec(2, 2, 0, e1);
		assertEquals(2, targetScalar("SELECT count(1) from utest.etl5t"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl5t WHERE NAME = 'a' AND ID = 45"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl5t WHERE NAME = 'b' AND ID = 11"));

		srcRunSql("INSERT INTO utest.etl5 (ID, NAME) VALUES (31, 'c')");
		srcRunSql("UPDATE utest.etl5 SET NAME = 'd' WHERE ID = 45");

		Execution e2 = task.run();
		assertExec(3, 1, 1, e2);
		assertEquals(3, targetScalar("SELECT count(1) from utest.etl5t"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl5t WHERE NAME = 'd' AND ID = 45"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl5t WHERE NAME = 'c' AND ID = 31"));

		srcRunSql("DELETE FROM utest.etl5 WHERE ID = 45");

		Execution e3 = task.run();
		assertExec(2, 0, 0, e3);
		assertEquals(3, targetScalar("SELECT count(1) from utest.etl5t"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl5t WHERE NAME = 'd' AND ID = 45"));

		Execution e4 = task.run();
		assertExec(2, 0, 0, e4);
	}

	@Test(expected = EtlRuntimeException.class)
	public void test_ById_Autoincrement() {

		EtlTask task = etl.getTaskService().createTaskBuilder(Etl1t.class)
				.withExtractor("com/nhl/link/etl/itest/etl1_to_etl1t_byid.xml").matchById(Etl1t.ID_PK_COLUMN).task();

		srcRunSql("INSERT INTO utest.etl1 (ID, NAME, AGE) VALUES (45, 'a', 67)");
		srcRunSql("INSERT INTO utest.etl1 (ID, NAME, AGE) VALUES (11, 'b', 4)");

		task.run();
	}

	@Test
	public void test_CapsLower() {

		EtlTask task = etl.getTaskService().createTaskBuilder(Etl1t.class)
				.withExtractor("com/nhl/link/etl/itest/etl1_to_etl1t_lower").matchBy(Etl1t.NAME).task();

		srcRunSql("INSERT INTO utest.etl1 (NAME, AGE) VALUES ('a', 3)");
		srcRunSql("INSERT INTO utest.etl1 (NAME, AGE) VALUES ('b', NULL)");

		Execution e1 = task.run();
		assertExec(2, 2, 0, e1);
		assertEquals(2, targetScalar("SELECT count(1) from utest.etl1t"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl1t WHERE NAME = 'a' AND age = 3"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl1t WHERE NAME = 'b' AND age is null"));

		srcRunSql("INSERT INTO utest.etl1 (NAME) VALUES ('c')");
		srcRunSql("UPDATE utest.etl1 SET AGE = 5 WHERE NAME = 'a'");
	}
}
