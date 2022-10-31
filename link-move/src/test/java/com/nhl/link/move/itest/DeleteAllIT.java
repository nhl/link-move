package com.nhl.link.move.itest;

import com.nhl.link.move.Execution;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.unit.LmIntegrationTest;
import com.nhl.link.move.unit.cayenne.t.Etl6t;

import org.junit.jupiter.api.Test;

class DeleteAllIT extends LmIntegrationTest {

	@Test
	void test_deleteAll() {

		LmTask task = lmRuntime.service(ITaskService.class)
				.deleteAll(Etl6t.class)
				.task();

		etl6t().insertColumns("id", "name")
				.values(1, "a")
				.values(2, "b")
				.values(3, "с")
				.exec();

		Execution e2 = task.run();
		assertExec(3, 0, 0, 3, e2);
		etl6t().matcher().assertNoMatches();
	}

	@Test
	void test_deleteAll_skipExecutionStats() {

		LmTask task = lmRuntime.service(ITaskService.class)
				.deleteAll(Etl6t.class)
				.skipExecutionStats()
				.task();

		etl6t().insertColumns("id", "name")
				.values(1, "a")
				.values(2, "b")
				.values(3, "с")
				.exec();

		Execution e2 = task.run();
		assertExec(0, 0, 0, 0, e2);
		etl6t().matcher().assertNoMatches();
	}

	@Test
	void test_deleteAll_withTargetFilter() {

		LmTask task = lmRuntime.service(ITaskService.class)
				.deleteAll(Etl6t.class)
				.targetFilter(Etl6t.NAME.startsWith("a"))
				.task();

		etl6t().insertColumns("id", "name")
				.values(1, "abc")
				.values(2, "acb")
				.values(3, "сba")
				.exec();

		Execution e2 = task.run();
		assertExec(2, 0, 0, 2, e2);

		etl6t().selectColumns("name").select().stream().map(it -> it[0]).forEach(System.out::println);

		etl6t().matcher().assertOneMatch();
		etl6t().matcher().eq("name", "сba").assertOneMatch();
	}
}
