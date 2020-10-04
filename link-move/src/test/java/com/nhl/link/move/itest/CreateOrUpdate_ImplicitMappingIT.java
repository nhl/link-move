package com.nhl.link.move.itest;

import com.nhl.link.move.Execution;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.unit.LmIntegrationTest;
import com.nhl.link.move.unit.cayenne.t.Etl1t;
import com.nhl.link.move.unit.cayenne.t.Etl5t;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CreateOrUpdate_ImplicitMappingIT extends LmIntegrationTest {

	@Test
	public void test_ByDbAttribute() {
		test_MatchByKey("db:name");
	}

	@Test
	public void test_ByObjAttribute() {
		test_MatchByKey("name");
	}

	private void test_MatchByKey(String key) {

		LmTask task = lmRuntime.service(ITaskService.class).createOrUpdate(Etl1t.class)
				.sourceExtractor("com/nhl/link/move/itest/etl1_to_etl1t_implicit.xml").matchBy(key).task();

		srcEtl1().insertColumns("name", "age")
				.values("a", 3)
				.values("b", null)
				.exec();

		Execution e1 = task.run();
		assertExec(2, 2, 0, 0, e1);
		etl1t().matcher().assertMatches(2);
		etl1t().matcher().eq("NAME", "a").eq("AGE", 3).assertOneMatch();
		etl1t().matcher().eq("NAME", "b").eq("AGE", null).assertOneMatch();

		srcEtl1().insertColumns("name").values("c").exec();
		srcEtl1().update().set("age", 5).where("name", "a").exec();

		Execution e2 = task.run();
		assertExec(3, 1, 1, 0, e2);
		etl1t().matcher().assertMatches(3);
		etl1t().matcher().eq("NAME", "a").eq("AGE", 5).assertOneMatch();
		etl1t().matcher().eq("NAME", "c").eq("AGE", null).assertOneMatch();

		srcEtl1().delete().and("name", "a").exec();

		Execution e3 = task.run();
		assertExec(2, 0, 0, 0, e3);
		etl1t().matcher().assertMatches(3);
		etl1t().matcher().eq("NAME", "a").eq("AGE", 5).assertOneMatch();

		Execution e4 = task.run();
		assertExec(2, 0, 0, 0, e4);
	}

	@Test
	public void test_ById() {

		LmTask task = lmRuntime.service(ITaskService.class).createOrUpdate(Etl5t.class)
				.sourceExtractor("com/nhl/link/move/itest/etl5_to_etl5t_byid_implicit.xml")
				.matchById()
				.task();

		srcEtl5().insertColumns("id", "name").values(45, "a").values(11, "b").exec();

		Execution e1 = task.run();
		assertExec(2, 2, 0, 0, e1);
		assertEquals(2, targetScalar("SELECT count(1) from etl5t"));
		assertEquals(1, targetScalar("SELECT count(1) from etl5t WHERE NAME = 'a' AND ID = 45"));
		assertEquals(1, targetScalar("SELECT count(1) from etl5t WHERE NAME = 'b' AND ID = 11"));

		srcEtl5().insertColumns("id", "name").values(31, "c").exec();
		srcEtl5().update().set("name", "d").where("id", 45).exec();

		Execution e2 = task.run();
		assertExec(3, 1, 1, 0, e2);
		assertEquals(3, targetScalar("SELECT count(1) from etl5t"));
		assertEquals(1, targetScalar("SELECT count(1) from etl5t WHERE NAME = 'd' AND ID = 45"));
		assertEquals(1, targetScalar("SELECT count(1) from etl5t WHERE NAME = 'c' AND ID = 31"));

		srcEtl5().delete().and("id", 45).exec();

		Execution e3 = task.run();
		assertExec(2, 0, 0, 0, e3);
		assertEquals(3, targetScalar("SELECT count(1) from etl5t"));
		assertEquals(1, targetScalar("SELECT count(1) from etl5t WHERE NAME = 'd' AND ID = 45"));

		Execution e4 = task.run();
		assertExec(2, 0, 0, 0, e4);
	}
}
