package com.nhl.link.move.itest;

import com.nhl.link.move.Execution;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.unit.LmIntegrationTest;
import com.nhl.link.move.unit.cayenne.t.Etl7t;
import org.junit.jupiter.api.Test;

public class CreateOrUpdateIT_TransientProperties extends LmIntegrationTest {

    @Test
	public void test_ById() {

		LmTask task = lmRuntime.service(ITaskService.class)
				.createOrUpdate(Etl7t.class)
				.sourceExtractor("com/nhl/link/move/itest/etl7_to_etl7t_byid.xml")
				.matchById()
				.task();

		srcEtl7().insertColumns("id", "full_name", "sex")
				.values(1, "Lennon, John", "M")
				.values(2, "Hendrix, Jimi", "M")
				.values(3, "Joplin, Janis", "F")
				.exec();

		Execution e1 = task.run();
		assertExec(3, 3, 0, 0, e1);
		etl7t().matcher().assertMatches(3);
		etl7t().matcher().eq("SEX", null).assertMatches(3);
		etl7t().matcher().eq("id", 1).andEq("first_name", "John").andEq("last_name", "Lennon").assertOneMatch();
		etl7t().matcher().eq("id", 2).andEq("first_name", "Jimi").andEq("last_name", "Hendrix").assertOneMatch();
		etl7t().matcher().eq("id", 3).andEq("first_name", "Janis").andEq("last_name", "Joplin").assertOneMatch();
	}
}
