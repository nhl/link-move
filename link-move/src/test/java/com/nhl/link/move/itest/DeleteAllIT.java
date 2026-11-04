package com.nhl.link.move.itest;

import com.nhl.link.move.Execution;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.unit.LmIntegrationTest;
import com.nhl.link.move.unit.cayenne.t.Etl3t;
import com.nhl.link.move.unit.cayenne.t.Etl5t;
import com.nhl.link.move.unit.cayenne.t.Etl6t;
import org.apache.cayenne.CayenneRuntimeException;
import org.apache.cayenne.exp.ExpressionFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

class DeleteAllIT extends LmIntegrationTest {

	@Test
	void deleteAll() {

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
	void deleteAll_skipExecutionStats() {

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
	void deleteAll_withTargetFilter() {

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

	@Test
	void deleteAll_withTargetFilter_DbPathIn() {

		LmTask task = lmRuntime.service(ITaskService.class)
				.deleteAll(Etl6t.class)
				.targetFilter(ExpressionFactory.inDbExp("id", 1, 3))
				.task();

		etl6t().insertColumns("id", "name")
				.values(1, "abc")
				.values(2, "acb")
				.values(3, "cba")
				.exec();

		Execution e2 = task.run();
		assertExec(2, 0, 0, 2, e2);

		etl6t().matcher().assertOneMatch();
		etl6t().matcher().eq("id", 2).assertOneMatch();
	}

	@Test
	void deleteAll_withTargetFilter_Join_Unsupported() {

		LmTask task = lmRuntime.service(ITaskService.class)
				.deleteAll(Etl3t.class)
				.targetFilter(Etl3t.E5.dot(Etl5t.NAME).eq("a"))
				.task();

		assertThrows(CayenneRuntimeException.class, task::run);
	}
}
