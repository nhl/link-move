package com.nhl.link.move;

import com.nhl.link.move.extractor.model.ExtractorName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ExecutionTest {

    @Test
    public void testCreateReport() {

        Execution execution = new Execution("xsync", ExtractorName.create("l", "n"), Map.of("a", 5));

        assertEquals(Map.of(
                        "Task", "xsync",
                        "Parameter[a]", 5,
                        "Status", "in progress",
                        "Started on", execution.getStats().getStartedOn(),
                        "Extracted", 0L,
                        "Created", 0L,
                        "Updated", 0L,
                        "Deleted", 0L
                ),
                execution.createReport());

        execution.getStats().incrementCreated(5);
        execution.getStats().incrementDeleted(4);
        execution.getStats().incrementExtracted(55);
        execution.getStats().incrementUpdated(3);

        execution.close();

        assertEquals(Map.of(
                        "Task", "xsync",
                        "Parameter[a]", 5,
                        "Status", "finished",
                        "Started on", execution.getStats().getStartedOn(),
                        "Duration", execution.getStats().getDuration(),
                        "Extracted", 55L,
                        "Created", 5L,
                        "Updated", 3L,
                        "Deleted", 4L
                ),
                execution.createReport());
    }

    @Test
    public void testAttribute() {
        try (Execution execution = new Execution("xsync", ExtractorName.create("l", "n"), Map.of())) {
      
            assertNull(execution.getAttribute("a"));

            execution.setAttribute("a", "MMM");
            assertEquals("MMM", execution.getAttribute("a"));

            execution.setAttribute("a", null);
            assertNull(execution.getAttribute("a"));
        }
    }
}
