package com.nhl.link.move.itest;

import com.nhl.dflib.DataFrameBuilder;
import com.nhl.dflib.Index;
import com.nhl.link.move.Execution;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.annotation.AfterMissingTargetsFiltered;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.runtime.task.delete.DeleteSegment;
import com.nhl.link.move.runtime.task.delete.DeleteStage;
import com.nhl.link.move.unit.LmIntegrationTest;
import com.nhl.link.move.unit.cayenne.t.Etl1t;
import com.nhl.link.move.unit.cayenne.t.Etl6t;
import org.junit.jupiter.api.Test;

public class DeleteIT extends LmIntegrationTest {

	@Test
	public void test_EmptyTarget() {

		LmTask task = lmRuntime.service(ITaskService.class)
				.delete(Etl6t.class)
				.sourceMatchExtractor("com/nhl/link/move/itest/etl6_to_etl6t_byid.xml")
				.task();

		Execution e = task.run();
		assertExec(0, 0, 0, 0, e);
	}

	@Test
	public void test_ById_Normalized_IntegerToLong() {

		LmTask task = lmRuntime.service(ITaskService.class)
				.delete(Etl6t.class)
				.sourceMatchExtractor("com/nhl/link/move/itest/etl6_to_etl6t_byid.xml")
				.task();

		etl6t().insertColumns("id", "name")
				.values(1, "a")
				.values(2, "b")
				.exec();

		srcEtl6().insertColumns("id", "name").values(1, "a").exec();

		Execution e2 = task.run();
		assertExec(2, 0, 0, 1, e2);
		etl6t().matcher().assertOneMatch();
		etl6t().matcher().eq("name", "a").assertOneMatch();
	}

	@Test
	public void test_ByAttribute() {

		LmTask task = lmRuntime.service(ITaskService.class)
				.delete(Etl1t.class)
				.sourceMatchExtractor("com/nhl/link/move/itest/etl1_to_etl1t_upper.xml")
				.matchBy(Etl1t.NAME)
				.task();

		etl1t().insertColumns("name", "age")
				.values("a", 3)
				.values("b", null)
				.exec();

		Execution e1 = task.run();
		assertExec(2, 0, 0, 2, e1);

		etl1t().matcher().assertNoMatches();

		etl1t().insertColumns("name", "age")
				.values("a", 3)
				.values("b", null)
				.exec();

		srcEtl1().insertColumns("name").values("a").exec();

		Execution e2 = task.run();
		assertExec(2, 0, 0, 1, e2);

		etl1t().matcher().assertOneMatch();
		etl1t().matcher().eq("name", "a").assertOneMatch();
	}

	@Test
	public void test_ByAttribute_MultiBatch() {

		LmTask task = lmRuntime.service(ITaskService.class)
				.delete(Etl1t.class)
				.batchSize(2)
				.sourceMatchExtractor("com/nhl/link/move/itest/etl1_to_etl1t_upper.xml")
				.matchBy(Etl1t.NAME)
				.task();

		srcEtl1().insertColumns("name")
				.values("a")
				.values("b")
				.values("c")
				.values("d")
				.values("e")
				.exec();

		etl1t().insertColumns("name", "age")
				.values("a", 3)
				.values("d", null)
				.values("f", null)
				.values("g", null)
				.values("h", null)
				.exec();

		Execution e1 = task.run();
		assertExec(5, 0, 0, 3, e1);

		etl1t().matcher().assertMatches(2);
	}

	@Test
	public void test_CustomDeleteSet() {

		LmTask task = lmRuntime.service(ITaskService.class)
				.delete(Etl6t.class)
				.sourceMatchExtractor("com/nhl/link/move/itest/etl6_to_etl6t_byid.xml")
				.stageListener(new CustomDeleteSetListener())
				.task();

		etl6t().insertColumns("id", "name")
				.values(1, "a")
				.values(2, "b")
				.exec();

		Execution e2 = task.run();
		assertExec(2, 0, 0, 0, e2);
		etl6t().matcher().assertMatches(2);
	}

	@Test
	public void test_LambdaBasedCallback_filterMissingTargets() {

		LmTask task = lmRuntime.service(ITaskService.class)
				.delete(Etl6t.class)
				.sourceMatchExtractor("com/nhl/link/move/itest/etl6_to_etl6t_byid.xml")
				.stage(DeleteStage.FILTER_MISSING_TARGETS, (execution, segment) -> {
					Index index = segment.getMissingTargets().getColumnsIndex();
					segment.setMissingTargets(DataFrameBuilder.builder(index).empty());
				})
				.task();

		etl6t().insertColumns("id", "name")
				.values(1, "a")
				.values(2, "b")
				.exec();

		Execution e2 = task.run();
		assertExec(2, 0, 0, 0, e2);
		etl6t().matcher().assertMatches(2);
	}


	public static class CustomDeleteSetListener {

		@AfterMissingTargetsFiltered
		public void afterMissingTargetsFiltered(Execution exec, DeleteSegment segment) {
			Index index = segment.getMissingTargets().getColumnsIndex();
			segment.setMissingTargets(DataFrameBuilder.builder(index).empty());
		}
	}
}
