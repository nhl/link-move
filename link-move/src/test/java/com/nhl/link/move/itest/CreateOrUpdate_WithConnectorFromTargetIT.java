package com.nhl.link.move.itest;

import com.nhl.link.move.Execution;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.runtime.LmRuntime;
import com.nhl.link.move.runtime.LmRuntimeBuilder;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.unit.LmIntegrationTest;
import com.nhl.link.move.unit.cayenne.t.Etl2t;
import org.junit.jupiter.api.Test;

public class CreateOrUpdate_WithConnectorFromTargetIT extends LmIntegrationTest {

	@Override
	protected LmRuntimeBuilder testRuntimeBuilder() {
		return LmRuntime.builder()
				// override connector logic defined in super to make source and target the same DB
				.connectorFromTarget()
				.targetRuntime(targetCayenne.getRuntime());
	}

	@Test
	public void byAttribute() {

		LmTask task = lmRuntime.service(ITaskService.class)
				.createOrUpdate(Etl2t.class)
				.sourceExtractor("com/nhl/link/move/itest/etl1t_to_etl2t.xml")
				.matchBy(Etl2t.NAME)
				.task();

		etl1t().insertColumns("name").values("a").values("b").exec();

		Execution e1 = task.run();
		assertExec(2, 2, 0, 0, e1);

		etl2t().matcher().assertMatches(2);
		etl2t().matcher().eq("name", "a").eq("address", null).assertOneMatch();
		etl2t().matcher().eq("name", "b").eq("address", null).assertOneMatch();

		etl1t().insertColumns("name").values("c").exec();
		etl1t().update().set("name", "d").where("name", "a").exec();

		Execution e2 = task.run();
		assertExec(3, 2, 0, 0, e2);
		etl2t().matcher().assertMatches(4);
		etl2t().matcher().eq("name", "a").eq("address", null).assertOneMatch();
		etl2t().matcher().eq("name", "b").eq("address", null).assertOneMatch();
		etl2t().matcher().eq("name", "c").eq("address", null).assertOneMatch();
		etl2t().matcher().eq("name", "d").eq("address", null).assertOneMatch();
	}
}
