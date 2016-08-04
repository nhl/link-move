package com.nhl.link.move.itest;

import static org.junit.Assert.assertEquals;

import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.unit.cayenne.t.Etl9t;
import org.junit.Test;

import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.Execution;
import com.nhl.link.move.unit.LmIntegrationTest;
import com.nhl.link.move.unit.cayenne.t.Etl1t;
import com.nhl.link.move.unit.cayenne.t.Etl3t;
import com.nhl.link.move.unit.cayenne.t.Etl5t;

public class CreateOrUpdateIT extends LmIntegrationTest {

	@Test
	public void test_ByAttribute_v1() {

		LmTask task = etl.service(ITaskService.class).createOrUpdate(Etl1t.class)
				.sourceExtractor("com/nhl/link/move/itest/etl1_to_etl1t").matchBy(Etl1t.NAME).task();

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
	public void test_ByAttribute() {

		LmTask task = etl.service(ITaskService.class).createOrUpdate(Etl1t.class)
				.sourceExtractor("com/nhl/link/move/itest/etl1_to_etl1t").matchBy(Etl1t.NAME).task();

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
	public void test_ByDbAttribute() {

		LmTask task = etl.service(ITaskService.class).createOrUpdate(Etl1t.class)
				.sourceExtractor("com/nhl/link/move/itest/etl1_to_etl1t").matchBy("db:name").task();

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
	public void test_ByAttributes() {

		LmTask task = etl.service(ITaskService.class).createOrUpdate(Etl1t.class)
				.sourceExtractor("com/nhl/link/move/itest/etl1_to_etl1t").matchBy(Etl1t.NAME, Etl1t.AGE).task();

		srcRunSql("INSERT INTO utest.etl1 (NAME, AGE) VALUES ('a', 3)");
		srcRunSql("INSERT INTO utest.etl1 (NAME, AGE) VALUES ('b', 5)");

		Execution e1 = task.run();
		assertExec(2, 2, 0, 0, e1);
		assertEquals(2, targetScalar("SELECT count(1) from utest.etl1t"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl1t WHERE NAME = 'a' AND age = 3"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl1t WHERE NAME = 'b' AND age = 5"));

		// changing one of the key components should result in no-match and a
		// new record insertion
		srcRunSql("UPDATE utest.etl1 SET NAME = 'c' WHERE NAME = 'a'");

		Execution e2 = task.run();
		assertExec(2, 1, 0, 0, e2);
		assertEquals(3, targetScalar("SELECT count(1) from utest.etl1t"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl1t WHERE NAME = 'c' AND age = 3"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl1t WHERE NAME = 'b' AND age = 5"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl1t WHERE NAME = 'a' AND age = 3"));

		Execution e4 = task.run();
		assertExec(2, 0, 0, 0, e4);
	}

	@Test
	public void test_ById() {

		LmTask task = etl.service(ITaskService.class).createOrUpdate(Etl5t.class)
				.sourceExtractor("com/nhl/link/move/itest/etl5_to_etl5t_byid.xml").matchById().task();

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

	@Test
	public void test_ById_Default() {

		// not specifying "matchById" explicitly ... this should be the default
		LmTask task = etl.service(ITaskService.class).createOrUpdate(Etl5t.class)
				.sourceExtractor("com/nhl/link/move/itest/etl5_to_etl5t_byid.xml").task();

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

	@Test(expected = LmRuntimeException.class)
	public void test_ById_Autoincrement() {

		LmTask task = etl.service(ITaskService.class).createOrUpdate(Etl1t.class)
				.sourceExtractor("com/nhl/link/move/itest/etl1_to_etl1t_byid.xml").matchById().task();

		srcRunSql("INSERT INTO utest.etl1 (ID, NAME, AGE) VALUES (45, 'a', 67)");
		srcRunSql("INSERT INTO utest.etl1 (ID, NAME, AGE) VALUES (11, 'b', 4)");

		task.run();
	}
	
	@Test
	public void test_CapsLower() {

		LmTask task = etl.service(ITaskService.class).createOrUpdate(Etl1t.class)
				.sourceExtractor("com/nhl/link/move/itest/etl1_to_etl1t_lower").matchBy(Etl1t.NAME).task();

		srcRunSql("INSERT INTO utest.etl1 (NAME, AGE) VALUES ('a', 3)");
		srcRunSql("INSERT INTO utest.etl1 (NAME, AGE) VALUES ('b', NULL)");

		Execution e1 = task.run();
		assertExec(2, 2, 0, 0, e1);
		assertEquals(2, targetScalar("SELECT count(1) from utest.etl1t"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl1t WHERE NAME = 'a' AND age = 3"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl1t WHERE NAME = 'b' AND age is null"));
	}

	@Test
	public void test_ExtraSrcColumns() {

		LmTask task = etl.service(ITaskService.class).createOrUpdate(Etl1t.class)
				.sourceExtractor("com/nhl/link/move/itest/etl1_to_etl1t_extra_source_columns").matchBy(Etl1t.NAME).task();

		srcRunSql("INSERT INTO utest.etl1 (NAME, DESCRIPTION) VALUES ('a', 'dd')");
		srcRunSql("INSERT INTO utest.etl1 (NAME, DESCRIPTION) VALUES ('b', NULL)");

		Execution e1 = task.run();
		assertExec(2, 2, 0, 0, e1);
		assertEquals(2, targetScalar("SELECT count(1) from utest.etl1t"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl1t WHERE NAME = 'a' AND DESCRIPTION is null AND AGE is null"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl1t WHERE NAME = 'b' AND DESCRIPTION is null AND AGE is null"));
	}

	@Test
	public void test_ByAttribute_SyncFk() {

		LmTask task = etl.service(ITaskService.class).createOrUpdate(Etl3t.class)
				.sourceExtractor("com/nhl/link/move/itest/etl3_to_etl3t").matchBy(Etl3t.NAME).task();

		srcRunSql("INSERT INTO utest.etl2 (ID, ADDRESS, NAME) VALUES (34, 'Address1', '2Name1')");
		srcRunSql("INSERT INTO utest.etl2 (ID, ADDRESS, NAME) VALUES (58, 'Address2', '2Name2')");
		srcRunSql("INSERT INTO utest.etl5 (ID, NAME) VALUES (17, '5Name1')");
		srcRunSql("INSERT INTO utest.etl5 (ID, NAME) VALUES (11, '5Name2')");
		srcRunSql("INSERT INTO utest.etl3 (E2_ID, E5_ID, NAME, PHONE_NUMBER) VALUES (58, 17, '3Name1', '3PHONE1')");
		srcRunSql("INSERT INTO utest.etl3 (E2_ID, E5_ID, NAME, PHONE_NUMBER) VALUES (34, 17, '3Name2', '3PHONE2')");

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

	@Test
	public void test_ByAttribute_SyncFk_Nulls() {

		LmTask task = etl.service(ITaskService.class).createOrUpdate(Etl3t.class)
				.sourceExtractor("com/nhl/link/move/itest/etl3_to_etl3t").matchBy(Etl3t.NAME).task();

		srcRunSql("INSERT INTO utest.etl2 (ID, ADDRESS, NAME) VALUES (34, 'Address1', '2Name1')");
		srcRunSql("INSERT INTO utest.etl5 (ID, NAME) VALUES (17, '5Name1')");
		srcRunSql("INSERT INTO utest.etl5 (ID, NAME) VALUES (11, '5Name2')");

		srcRunSql("INSERT INTO utest.etl3 (E2_ID, E5_ID, NAME, PHONE_NUMBER) VALUES (null, 17, '3Name1', '3PHONE1')");
		srcRunSql("INSERT INTO utest.etl3 (E2_ID, E5_ID, NAME, PHONE_NUMBER) VALUES (34, null, '3Name2', '3PHONE2')");

		targetRunSql("INSERT INTO utest.etl2t (ID, ADDRESS, NAME) VALUES (34, 'Address1', '2Name1')");
		targetRunSql("INSERT INTO utest.etl5t (ID, NAME) VALUES (17, '5Name1')");
		targetRunSql("INSERT INTO utest.etl5t (ID, NAME) VALUES (11, '5Name2')");

		Execution e1 = task.run();
		assertExec(2, 2, 0, 0, e1);
		assertEquals(2, targetScalar("SELECT count(1) from utest.etl3t"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl3t "
				+ "WHERE E2_ID IS NULL AND E5_ID = 17 AND NAME = '3Name1' AND phone_number = '3PHONE1'"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl3t "
				+ "WHERE E2_ID = 34 AND E5_ID IS NULL AND NAME = '3Name2' AND phone_number = '3PHONE2'"));

		srcRunSql("UPDATE utest.etl3 SET E5_ID = NULL WHERE NAME = '3Name1'");
		srcRunSql("UPDATE utest.etl3 SET E5_ID = 11 WHERE NAME = '3Name2'");

		Execution e2 = task.run();
		assertExec(2, 0, 2, 0, e2);
		assertEquals(2, targetScalar("SELECT count(1) from utest.etl3t"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl3t "
				+ "WHERE E2_ID IS NULL AND E5_ID IS NULL AND NAME = '3Name1' AND phone_number = '3PHONE1'"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl3t "
				+ "WHERE E2_ID = 34 AND E5_ID = 11 AND NAME = '3Name2' AND phone_number = '3PHONE2'"));
	}

	@Test
	public void test_ByAttribute_SyncNulls() {

		LmTask task = etl.service(ITaskService.class).createOrUpdate(Etl1t.class)
				.sourceExtractor("com/nhl/link/move/itest/etl1_to_etl1t").matchBy(Etl1t.NAME).task();

		targetRunSql("INSERT INTO utest.etl1t (NAME, AGE) VALUES ('a', 3)");
		srcRunSql("INSERT INTO utest.etl1 (NAME, AGE) VALUES ('a', NULL)");

		Execution e1 = task.run();
		assertExec(1, 0, 1, 0, e1);
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl1t"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl1t WHERE NAME = 'a' AND age is null"));
	}

	@Test
	public void test_ById_RelationshipOnPK() {

		// remove auto_pk_support so that commit would fail if ID is absent
		targetRunSql("DROP TABLE AUTO_PK_SUPPORT");

		LmTask task = etl.service(ITaskService.class).createOrUpdate(Etl9t.class)
				.sourceExtractor("com/nhl/link/move/itest/etl9_to_etl9t.xml").matchById().task();

		srcRunSql("INSERT INTO utest.etl9 (ID, NAME) VALUES (1, 'a')");
		srcRunSql("INSERT INTO utest.etl9 (ID, NAME) VALUES (2, 'b')");

		Execution e1 = task.run();
		assertExec(2, 2, 0, 0, e1);
	}
}
