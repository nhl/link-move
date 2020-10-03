package com.nhl.link.move.itest;

import com.nhl.link.move.Execution;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.runtime.task.sourcekeys.SourceKeysTask;
import com.nhl.link.move.unit.LmIntegrationTest;
import com.nhl.link.move.unit.cayenne.t.Etl1t;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class SourceKeysIT extends LmIntegrationTest {

	@SuppressWarnings("unchecked")
	@Test
	public void test_ByAttribute() {

		LmTask task = etl.service(ITaskService.class).extractSourceKeys(Etl1t.class)
				.sourceExtractor("com/nhl/link/move/itest/etl1_to_etl1t.xml").matchBy("name").task();

		srcRunSql("INSERT INTO utest.etl1 (NAME, AGE) VALUES ('a', 3)");
		srcRunSql("INSERT INTO utest.etl1 (NAME, AGE) VALUES ('b', NULL)");

		Execution e1 = task.run();
		assertExec(2, 0, 0, 0, e1);
		Set<Object> keys1 = (Set<Object>) e1.getAttribute(SourceKeysTask.RESULT_KEY);
		assertNotNull(keys1);
		assertEquals(new HashSet<>(Arrays.asList("a", "b")), keys1);

		srcRunSql("INSERT INTO utest.etl1 (NAME) VALUES ('c')");
		srcRunSql("UPDATE utest.etl1 SET NAME = 'd' WHERE NAME = 'a'");

		Execution e2 = task.run();
		assertExec(3, 0, 0, 0, e2);
		Set<Object> keys2 = (Set<Object>) e2.getAttribute(SourceKeysTask.RESULT_KEY);
		assertNotNull(keys2);
		assertEquals(new HashSet<>(Arrays.asList("b", "c", "d")), keys2);
	}
}
