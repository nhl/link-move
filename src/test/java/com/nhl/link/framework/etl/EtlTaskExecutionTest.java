package com.nhl.link.framework.etl;

import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.junit.Test;

public class EtlTaskExecutionTest {

	@Test
	public void testCreateReport() {

		SyncToken token = new IntToken("xsync", 5);
		Execution execution = new Execution(token);

		Map<String, Object> r1 = execution.createReport();
		assertEquals("xsync", r1.get("Task"));
		assertEquals(5, r1.get("Token"));
		assertEquals(r1.toString(), 7, r1.size());
		assertEquals("in progress", r1.get("Status"));

		execution.close();

		Map<String, Object> r2 = execution.createReport();
		assertEquals(r2.toString(), 7, r2.size());
		assertEquals("finished", r2.get("Status"));

	}
}
