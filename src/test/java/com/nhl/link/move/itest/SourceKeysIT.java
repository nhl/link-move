package com.nhl.link.move.itest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import com.nhl.link.move.EtlTask;
import com.nhl.link.move.Execution;
import com.nhl.link.move.runtime.task.sourcekeys.SourceKeysTask;
import com.nhl.link.move.unit.EtlIntegrationTest;
import com.nhl.link.move.unit.cayenne.t.Etl1t;

public class SourceKeysIT extends EtlIntegrationTest {

	@SuppressWarnings("unchecked")
	@Test
	public void test_ByAttribute() {

		EtlTask task = etl.getTaskService().extractSourceKeys(Etl1t.class)
				.sourceExtractor("com/nhl/link/move/itest/etl1_to_etl1t").matchBy("name").task();

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
