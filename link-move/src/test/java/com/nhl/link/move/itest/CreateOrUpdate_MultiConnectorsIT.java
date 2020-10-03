package com.nhl.link.move.itest;

import com.nhl.link.move.Execution;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.unit.LmIntegrationTest;
import com.nhl.link.move.unit.cayenne.t.Etl1t;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CreateOrUpdate_MultiConnectorsIT extends LmIntegrationTest {

    @Test
	public void testMultiConnectors_XMLSource() {

		LmTask task = lmRuntime.service(ITaskService.class).createOrUpdate(Etl1t.class)
				.sourceExtractor("com/nhl/link/move/itest/etl1_to_etl1t_multiconnectors.xml").matchBy(Etl1t.NAME).task();

		Execution e1 = task.run();
		assertExec(3, 3, 0, 0, e1);
		assertEquals(3, targetScalar("SELECT count(1) from etl1t"));
		assertEquals(1, targetScalar("SELECT count(1) from etl1t WHERE NAME = 'xxx' AND age = 1"));
		assertEquals(1, targetScalar("SELECT count(1) from etl1t WHERE NAME = 'yyy' AND age = 2"));
        assertEquals(1, targetScalar("SELECT count(1) from etl1t WHERE NAME = 'zzz' AND age = 3"));
	}
}
