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

public class CreateOrUpdate_DryRunIT extends LmIntegrationTest {

    @Test
	public void test_WithoutParameters() {
		LmTask task = etl.service(ITaskService.class).createOrUpdate(Etl1t.class)
				.sourceExtractor("com/nhl/link/move/itest/etl1_to_etl1t").matchBy(Etl1t.NAME).task();

		srcRunSql("INSERT INTO utest.etl1 (NAME, AGE) VALUES ('a', 3)");
		srcRunSql("INSERT INTO utest.etl1 (NAME, AGE) VALUES ('b', NULL)");

		Execution e1 = task.dryRun();
		assertExec(2, 2, 0, 0, e1);
		assertEquals(0, targetScalar("SELECT count(1) from utest.etl1t"));
	}

    @Test
	public void test_WithParameters() {
		LmTask task = etl.service(ITaskService.class).createOrUpdate(Etl1t.class)
				.sourceExtractor("com/nhl/link/move/itest/etl1_to_etl1t").matchBy(Etl1t.NAME).task();

		srcRunSql("INSERT INTO utest.etl1 (NAME, AGE) VALUES ('a', 3)");
		srcRunSql("INSERT INTO utest.etl1 (NAME, AGE) VALUES ('b', NULL)");

		Execution e1 = task.dryRun(Collections.singletonMap("x", "y"));
		assertExec(2, 2, 0, 0, e1);
		assertEquals(0, targetScalar("SELECT count(1) from utest.etl1t"));
	}

    @Test
	public void test_SyncToken_WithoutParameters() {
		LmTask task = etl.service(ITaskService.class).createOrUpdate(Etl1t.class)
				.sourceExtractor("com/nhl/link/move/itest/etl1_to_etl1t").matchBy(Etl1t.NAME).task();

		srcRunSql("INSERT INTO utest.etl1 (NAME, AGE) VALUES ('a', 3)");
		srcRunSql("INSERT INTO utest.etl1 (NAME, AGE) VALUES ('b', NULL)");

		Execution e1 = task.dryRun(new TimestampToken("token"));
		assertExec(2, 2, 0, 0, e1);
		assertEquals(0, targetScalar("SELECT count(1) from utest.etl1t"));
	}

    @Test
	public void test_SyncToken_WithParameters() {
		LmTask task = etl.service(ITaskService.class).createOrUpdate(Etl1t.class)
				.sourceExtractor("com/nhl/link/move/itest/etl1_to_etl1t").matchBy(Etl1t.NAME).task();

		srcRunSql("INSERT INTO utest.etl1 (NAME, AGE) VALUES ('a', 3)");
		srcRunSql("INSERT INTO utest.etl1 (NAME, AGE) VALUES ('b', NULL)");

		Execution e1 = task.dryRun(new TimestampToken("token"), Collections.singletonMap("x", "y"));
		assertExec(2, 2, 0, 0, e1);
		assertEquals(0, targetScalar("SELECT count(1) from utest.etl1t"));
	}
}
