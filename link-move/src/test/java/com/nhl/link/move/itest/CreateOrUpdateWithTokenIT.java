package com.nhl.link.move.itest;

import com.nhl.link.move.Execution;
import com.nhl.link.move.IntToken;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.unit.LmIntegrationTest;
import com.nhl.link.move.unit.cayenne.t.Etl1t;
import org.junit.jupiter.api.Test;

public class CreateOrUpdateWithTokenIT extends LmIntegrationTest {

	@Test
	public void test_ByAttribute() {

		LmTask task = lmRuntime.service(ITaskService.class)
				.createOrUpdate(Etl1t.class)
				.sourceExtractor("com/nhl/link/move/itest/etl1_to_etl1t_withtoken.xml")
				.matchBy(Etl1t.NAME)
				.task();

		srcEtl1().insertColumns("name", "age")
				.values("a", 3)
				.values("b", 1)
				.exec();

		Execution e1 = task.run(new IntToken("test_ByAttribute", 2));
		assertExec(1, 1, 0, 0, e1);
		etl1t().matcher().assertOneMatch();
		etl1t().matcher().eq("name", "b").eq("age", 1).assertOneMatch();

		Execution e2 = task.run(new IntToken("test_ByAttribute", 5));
		assertExec(1, 1, 0, 0, e2);
		etl1t().matcher().assertMatches(2);
		etl1t().matcher().eq("name", "a").eq("age", 3).assertOneMatch();

		Execution e3 = task.run(new IntToken("test_ByAttribute", 8));
		assertExec(0, 0, 0, 0, e3);

		srcEtl1().update().set("age", 9).where("name", "b").exec();

		Execution e4 = task.run(new IntToken("test_ByAttribute", 11));
		assertExec(1, 0, 1, 0, e4);
		etl1t().matcher().assertMatches(2);
		etl1t().matcher().eq("name", "b").eq("age", 9).assertOneMatch();
	}
}
