package com.nhl.link.move.itest;

import com.nhl.link.move.Execution;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.TimestampToken;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.unit.LmIntegrationTest;
import com.nhl.link.move.unit.cayenne.t.Etl1t;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class Delete_DryRunIT extends LmIntegrationTest {

    @Test
	public void test_WithoutParameters() {
		LmTask task = etl.service(ITaskService.class).delete(Etl1t.class)
				.sourceMatchExtractor("com/nhl/link/move/itest/etl1_to_etl1t").matchBy(Etl1t.NAME).task();

		targetRunSql("INSERT INTO utest.etl1t (NAME, AGE) VALUES ('a', 3)");
		targetRunSql("INSERT INTO utest.etl1t (NAME, AGE) VALUES ('b', NULL)");

		Execution e1 = task.dryRun();
		assertExec(0, 0, 0, 2, e1);

		assertEquals(2, targetScalar("SELECT count(1) from utest.etl1t"));
	}

    @Test
	public void test_WithParameters() {
		LmTask task = etl.service(ITaskService.class).delete(Etl1t.class)
				.sourceMatchExtractor("com/nhl/link/move/itest/etl1_to_etl1t").matchBy(Etl1t.NAME).task();

		targetRunSql("INSERT INTO utest.etl1t (NAME, AGE) VALUES ('a', 3)");
		targetRunSql("INSERT INTO utest.etl1t (NAME, AGE) VALUES ('b', NULL)");

		Execution e1 = task.dryRun(Collections.singletonMap("x", "y"));
		assertExec(0, 0, 0, 2, e1);

		assertEquals(2, targetScalar("SELECT count(1) from utest.etl1t"));
	}

    @Test
	public void test_SyncToken_WithoutParameters() {
		LmTask task = etl.service(ITaskService.class).delete(Etl1t.class)
				.sourceMatchExtractor("com/nhl/link/move/itest/etl1_to_etl1t").matchBy(Etl1t.NAME).task();

		targetRunSql("INSERT INTO utest.etl1t (NAME, AGE) VALUES ('a', 3)");
		targetRunSql("INSERT INTO utest.etl1t (NAME, AGE) VALUES ('b', NULL)");

		Execution e1 = task.dryRun(new TimestampToken("token"));
		assertExec(0, 0, 0, 2, e1);

		assertEquals(2, targetScalar("SELECT count(1) from utest.etl1t"));
	}

    @Test
	public void test_SyncToken_WithParameters() {
		LmTask task = etl.service(ITaskService.class).delete(Etl1t.class)
				.sourceMatchExtractor("com/nhl/link/move/itest/etl1_to_etl1t").matchBy(Etl1t.NAME).task();

		targetRunSql("INSERT INTO utest.etl1t (NAME, AGE) VALUES ('a', 3)");
		targetRunSql("INSERT INTO utest.etl1t (NAME, AGE) VALUES ('b', NULL)");

		Execution e1 = task.dryRun(new TimestampToken("token"), Collections.singletonMap("x", "y"));
		assertExec(0, 0, 0, 2, e1);

		assertEquals(2, targetScalar("SELECT count(1) from utest.etl1t"));
	}
}
