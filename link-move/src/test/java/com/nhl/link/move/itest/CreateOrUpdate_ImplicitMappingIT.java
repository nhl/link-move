package com.nhl.link.move.itest;

import com.nhl.link.move.Execution;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.unit.LmIntegrationTest;
import com.nhl.link.move.unit.cayenne.t.Etl1t;
import com.nhl.link.move.unit.cayenne.t.Etl5t;
import org.junit.jupiter.api.Test;

public class CreateOrUpdate_ImplicitMappingIT extends LmIntegrationTest {

	@Test
	public void byDbAttribute() {
		matchByKey("db:name");
	}

	@Test
	public void byObjAttribute() {
		matchByKey("name");
	}

	private void matchByKey(String key) {

		LmTask task = lmRuntime.service(ITaskService.class)
				.createOrUpdate(Etl1t.class)
				.sourceExtractor("com/nhl/link/move/itest/etl1_to_etl1t_implicit.xml")
				.matchBy(key)
				.task();

		srcEtl1().insertColumns("name", "age")
				.values("a", 3)
				.values("b", null)
				.exec();

		Execution e1 = task.run();
		assertExec(2, 2, 0, 0, e1);
		etl1t().matcher().assertMatches(2);
		etl1t().matcher().eq("name", "a").eq("age", 3).assertOneMatch();
		etl1t().matcher().eq("name", "b").eq("age", null).assertOneMatch();

		srcEtl1().insertColumns("name").values("c").exec();
		srcEtl1().update().set("age", 5).where("name", "a").exec();

		Execution e2 = task.run();
		assertExec(3, 1, 1, 0, e2);
		etl1t().matcher().assertMatches(3);
		etl1t().matcher().eq("name", "a").eq("age", 5).assertOneMatch();
		etl1t().matcher().eq("name", "c").eq("age", null).assertOneMatch();

		srcEtl1().delete().where("name", "a").exec();

		Execution e3 = task.run();
		assertExec(2, 0, 0, 0, e3);
		etl1t().matcher().assertMatches(3);
		etl1t().matcher().eq("name", "a").eq("age", 5).assertOneMatch();

		Execution e4 = task.run();
		assertExec(2, 0, 0, 0, e4);
	}

	@Test
	public void byId() {

		LmTask task = lmRuntime.service(ITaskService.class).createOrUpdate(Etl5t.class)
				.sourceExtractor("com/nhl/link/move/itest/etl5_to_etl5t_byid_implicit.xml")
				.matchById()
				.task();

		srcEtl5().insertColumns("id", "name").values(45, "a").values(11, "b").exec();

		Execution e1 = task.run();
		assertExec(2, 2, 0, 0, e1);

		etl5t().matcher().assertMatches(2);
		etl5t().matcher().eq("name", "a").eq("id", 45).assertOneMatch();
		etl5t().matcher().eq("name", "b").eq("id", 11).assertOneMatch();

		srcEtl5().insertColumns("id", "name").values(31, "c").exec();
		srcEtl5().update().set("name", "d").where("id", 45).exec();

		Execution e2 = task.run();
		assertExec(3, 1, 1, 0, e2);
		etl5t().matcher().assertMatches(3);
		etl5t().matcher().eq("name", "d").eq("id", 45).assertOneMatch();
		etl5t().matcher().eq("name", "c").eq("id", 31).assertOneMatch();

		srcEtl5().delete().where("id", 45).exec();

		Execution e3 = task.run();
		assertExec(2, 0, 0, 0, e3);
		etl5t().matcher().assertMatches(3);
		etl5t().matcher().eq("name", "d").eq("id", 45).assertOneMatch();

		Execution e4 = task.run();
		assertExec(2, 0, 0, 0, e4);
	}
}
