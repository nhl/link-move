package com.nhl.link.move.itest;

import com.nhl.link.move.Execution;
import com.nhl.link.move.IntToken;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.unit.LmIntegrationTest;
import com.nhl.link.move.unit.cayenne.t.Etl1t;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CreateOrUpdateWithTokenIT extends LmIntegrationTest {

	@Test
	public void test_ByAttribute() {

		LmTask task = etl.service(ITaskService.class).createOrUpdate(Etl1t.class)
				.sourceExtractor("com/nhl/link/move/itest/etl1_to_etl1t_withtoken.xml").matchBy(Etl1t.NAME).task();

		srcEtl1().insertColumns("name", "age")
				.values("a", 3)
				.values("b", 1)
				.exec();

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

		srcEtl1().update().set("age", 9).where("name", "b").exec();

		Execution e4 = task.run(new IntToken("test_ByAttribute", 11));
		assertExec(1, 0, 1, 0, e4);
		assertEquals(2, targetScalar("SELECT count(1) from utest.etl1t"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl1t WHERE NAME = 'b' AND age = 9"));
	}

}
