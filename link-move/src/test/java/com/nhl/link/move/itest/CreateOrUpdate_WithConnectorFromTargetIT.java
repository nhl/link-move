package com.nhl.link.move.itest;

import com.nhl.link.move.Execution;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.runtime.LmRuntime;
import com.nhl.link.move.runtime.LmRuntimeBuilder;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.unit.LmIntegrationTest;
import com.nhl.link.move.unit.cayenne.t.Etl2t;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CreateOrUpdate_WithConnectorFromTargetIT extends LmIntegrationTest {

	@Override
	protected LmRuntime createLmRuntime() {
		// override connector logic defined in super to make source and target the same DB
		return new LmRuntimeBuilder().withConnectorFromTarget().withTargetRuntime(targetCayenne.getRuntime()).build();
	}

	@Test
	public void test_ByAttribute() {

		LmTask task = lmRuntime.service(ITaskService.class).createOrUpdate(Etl2t.class)
				.sourceExtractor("com/nhl/link/move/itest/etl1t_to_etl2t.xml").matchBy(Etl2t.NAME).task();

		targetRunSql("INSERT INTO etl1t (NAME) VALUES ('a')");
		targetRunSql("INSERT INTO etl1t (NAME) VALUES ('b')");

		Execution e1 = task.run();
		assertExec(2, 2, 0, 0, e1);
		assertEquals(2, targetScalar("SELECT count(1) from etl2t"));
		assertEquals(1, targetScalar("SELECT count(1) from etl2t WHERE NAME = 'a' AND address is null"));
		assertEquals(1, targetScalar("SELECT count(1) from etl2t WHERE NAME = 'b' AND address is null"));

		targetRunSql("INSERT INTO etl1t (NAME) VALUES ('c')");
		targetRunSql("UPDATE etl1t SET NAME = 'd' WHERE NAME = 'a'");

		Execution e2 = task.run();
		assertExec(3, 2, 0, 0, e2);
		assertEquals(4, targetScalar("SELECT count(1) from etl2t"));
		assertEquals(1, targetScalar("SELECT count(1) from etl2t WHERE NAME = 'a' AND address is null"));
		assertEquals(1, targetScalar("SELECT count(1) from etl2t WHERE NAME = 'b' AND address is null"));
		assertEquals(1, targetScalar("SELECT count(1) from etl2t WHERE NAME = 'c' AND address is null"));
		assertEquals(1, targetScalar("SELECT count(1) from etl2t WHERE NAME = 'd' AND address is null"));
	}
}
