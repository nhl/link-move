package com.nhl.link.move.itest;

import com.nhl.link.move.Execution;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.unit.LmIntegrationTest;
import com.nhl.link.move.unit.cayenne.t.Etl7t;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
		assertEquals(3, targetScalar("SELECT count(1) from etl7t"));
        assertEquals(3, targetScalar("SELECT count(1) from etl7t WHERE SEX IS NULL"));
		assertEquals(1, targetScalar("SELECT count(1) from etl7t WHERE FIRST_NAME = 'John' AND LAST_NAME = 'Lennon' AND ID = 1"));
        assertEquals(1, targetScalar("SELECT count(1) from etl7t WHERE FIRST_NAME = 'Jimi' AND LAST_NAME = 'Hendrix' AND ID = 2"));
        assertEquals(1, targetScalar("SELECT count(1) from etl7t WHERE FIRST_NAME = 'Janis' AND LAST_NAME = 'Joplin' AND ID = 3"));
	}
}
