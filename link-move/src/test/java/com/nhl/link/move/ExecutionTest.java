package com.nhl.link.move;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ExecutionTest {

	@Test
	public void testCreateReport() {

		Execution execution = new Execution("xsync", Collections.singletonMap("a", 5));

		Map<String, Object> r1 = execution.createReport();
		assertEquals("xsync", r1.get("Task"));
		assertEquals(5, r1.get("Parameter[a]"));
		assertEquals(8, r1.size(), r1.toString());
		assertEquals("in progress", r1.get("Status"));

		execution.getStats().incrementCreated(5);
		execution.getStats().incrementDeleted(4);
		execution.getStats().incrementExtracted(55);
		execution.getStats().incrementUpdated(3);

		execution.close();

		Map<String, Object> r2 = execution.createReport();
		assertEquals(8, r2.size(), r2.toString());
		assertEquals("finished", r2.get("Status"));
		assertEquals(55L, r2.get("Extracted"));
		assertEquals(5L, r2.get("Created"));
		assertEquals(4L, r2.get("Deleted"));
		assertEquals(3L, r2.get("Updated"));
	}

	@Test
	public void testAttribute() {
		try (Execution execution = new Execution("xsync", Collections.emptyMap())) {
			assertNull(execution.getAttribute("a"));

			execution.setAttribute("a", "MMM");
			assertEquals("MMM", execution.getAttribute("a"));

			execution.setAttribute("a", null);
			assertNull(execution.getAttribute("a"));
		}
	}
}
