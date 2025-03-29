package com.nhl.link.move.itest;

import com.nhl.link.move.Execution;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.runtime.task.delete.DeleteStage;
import com.nhl.link.move.unit.LmIntegrationTest;
import com.nhl.link.move.unit.cayenne.t.Etl6t;
import org.dflib.DataFrame;
import org.junit.jupiter.api.Test;

public class Delete_Stage_CallbacksIT extends LmIntegrationTest {

	@Test
	public void customDeleteSet() {

		LmTask task = lmRuntime.service(ITaskService.class)
				.delete(Etl6t.class)
				.sourceMatchExtractor("com/nhl/link/move/itest/etl6_to_etl6t_byid.xml")
				.stage(DeleteStage.FILTER_MISSING_TARGETS, (e, s) -> s.setMissingTargets(DataFrame.empty(s.getMissingTargets().getColumnsIndex())))
				.task();

		etl6t().insertColumns("id", "name")
				.values(1, "a")
				.values(2, "b")
				.exec();

		Execution e2 = task.run();
		assertExec(2, 0, 0, 0, e2);
		etl6t().matcher().assertMatches(2);
	}
}
