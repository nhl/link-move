package com.nhl.link.move;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Collections;
import java.util.Map;

import org.junit.Test;

import com.nhl.link.move.Execution;

public class ExecutionTest {

	@Test
	public void testCreateReport() {

		Execution execution = new Execution("xsync", Collections.singletonMap("a", 5));

		Map<String, Object> r1 = execution.createReport();
		assertEquals("xsync", r1.get("Task"));
		assertEquals(5, r1.get("Parameter[a]"));
		assertEquals(r1.toString(), 8, r1.size());
		assertEquals("in progress", r1.get("Status"));

		execution.getStats().incrementCreated(5);
		execution.getStats().incrementDeleted(4);
		execution.getStats().incrementExtracted(55);
		execution.getStats().incrementUpdated(3);

		execution.close();

		Map<String, Object> r2 = execution.createReport();
		assertEquals(r2.toString(), 8, r2.size());
		assertEquals("finished", r2.get("Status"));
		assertEquals(55l, r2.get("Extracted"));
		assertEquals(5l, r2.get("Created"));
		assertEquals(4l, r2.get("Deleted"));
		assertEquals(3l, r2.get("Updated"));
	}

	@Test
	public void testAttribute() {
		try (Execution execution = new Execution("xsync", Collections.<String, Object> emptyMap())) {
			assertNull(execution.getAttribute("a"));

			execution.setAttribute("a", "MMM");
			assertEquals("MMM", execution.getAttribute("a"));

			execution.setAttribute("a", null);
			assertNull(execution.getAttribute("a"));
		}
	}
}
