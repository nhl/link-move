package com.nhl.link.etl.itest;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.nhl.link.etl.EtlTask;
import com.nhl.link.etl.Execution;
import com.nhl.link.etl.runtime.EtlRuntime;
import com.nhl.link.etl.runtime.EtlRuntimeBuilder;
import com.nhl.link.etl.unit.EtlIntegrationTest;
import com.nhl.link.etl.unit.cayenne.t.Etl2t;

public class CreateOrUpdate_TargetOnlyTest extends EtlIntegrationTest {

	@Override
	protected EtlRuntime createEtl() {
		// override connector logic defined in super to make source and target
		// the same DB
		return new EtlRuntimeBuilder().withConnectorFromTarget().withTargetRuntime(targetStack.runtime()).build();
	}

	@Test
	public void test_ByAttribute() {

		EtlTask task = etl.getTaskService().createTaskBuilder(Etl2t.class)
				.withExtractor("com/nhl/link/etl/itest/etl1t_to_etl2t").matchBy(Etl2t.NAME).task();

		targetRunSql("INSERT INTO utest.etl1t (NAME) VALUES ('a')");
		targetRunSql("INSERT INTO utest.etl1t (NAME) VALUES ('b')");

		Execution e1 = task.run();
		assertExec(2, 2, 0, e1);
		assertEquals(2, targetScalar("SELECT count(1) from utest.etl2t"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl2t WHERE NAME = 'a' AND address is null"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl2t WHERE NAME = 'b' AND address is null"));

		targetRunSql("INSERT INTO utest.etl1t (NAME) VALUES ('c')");
		targetRunSql("UPDATE utest.etl1t SET NAME = 'd' WHERE NAME = 'a'");

		Execution e2 = task.run();
		assertExec(3, 2, 0, e2);
		assertEquals(4, targetScalar("SELECT count(1) from utest.etl2t"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl2t WHERE NAME = 'a' AND address is null"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl2t WHERE NAME = 'b' AND address is null"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl2t WHERE NAME = 'c' AND address is null"));
		assertEquals(1, targetScalar("SELECT count(1) from utest.etl2t WHERE NAME = 'd' AND address is null"));
	}
}
