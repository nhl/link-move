package com.nhl.link.etl.itest;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.nhl.link.etl.EtlTask;
import com.nhl.link.etl.Execution;
import com.nhl.link.etl.IntToken;
import com.nhl.link.etl.unit.EtlIntegrationTest;
import com.nhl.link.etl.unit.cayenne.t.Etl1t;

public class CreateOrUpdateWithTokenIT extends EtlIntegrationTest {

	@Test
	public void test_ByAttribute() {

		EtlTask task = etl.getTaskService().createOrUpdate(Etl1t.class)
				.sourceExtractor("com/nhl/link/etl/itest/etl1_to_etl1t_withtoken").matchBy(Etl1t.NAME).task();

		srcRunSql("INSERT INTO utest.etl1 (NAME, AGE) VALUES ('a', 3)");
		srcRunSql("INSERT INTO utest.etl1 (NAME, AGE) VALUES ('b', 1)");

		Execution e1 = task.run(new IntToken("test_ByAttribute", 2));
		assertExec(1, 1, 0, 0, e1);
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl1t"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl1t WHERE NAME = 'b' AND age = 1"));

		Execution e2 = task.run(new IntToken("test_ByAttribute", 5));
		assertExec(1, 1, 0, 0, e2);
		assertEquals(2, targetScalar("SELECT count(1) from utest.etl1t"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl1t WHERE NAME = 'a' AND age = 3"));

		Execution e3 = task.run(new IntToken("test_ByAttribute", 8));
		assertExec(0, 0, 0, 0, e3);

		srcRunSql("UPDATE utest.etl1 SET AGE = 9 WHERE NAME = 'b'");

		Execution e4 = task.run(new IntToken("test_ByAttribute", 11));
		assertExec(1, 0, 1, 0, e4);
		assertEquals(2, targetScalar("SELECT count(1) from utest.etl1t"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl1t WHERE NAME = 'b' AND age = 9"));
	}

}
