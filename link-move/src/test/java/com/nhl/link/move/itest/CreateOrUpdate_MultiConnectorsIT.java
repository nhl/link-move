package com.nhl.link.move.itest;

import com.nhl.link.move.Execution;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.unit.LmIntegrationTest;
import com.nhl.link.move.unit.cayenne.t.Etl1t;
import org.junit.jupiter.api.Test;

public class CreateOrUpdate_MultiConnectorsIT extends LmIntegrationTest {

    @Test
	public void testMultiConnectors_XMLSource() {

		LmTask task = lmRuntime.service(ITaskService.class)
				.createOrUpdate(Etl1t.class)
				.sourceExtractor("com/nhl/link/move/itest/etl1_to_etl1t_multiconnectors.xml")
				.matchBy(Etl1t.NAME)
				.task();

		Execution e1 = task.run();
		assertExec(3, 3, 0, 0, e1);

		etl1t().matcher().assertMatches(3);
		etl1t().matcher().eq("name", "xxx").eq("age", 1).assertOneMatch();
		etl1t().matcher().eq("name", "yyy").eq("age", 2).assertOneMatch();
		etl1t().matcher().eq("name", "zzz").eq("age", 3).assertOneMatch();
	}
}
