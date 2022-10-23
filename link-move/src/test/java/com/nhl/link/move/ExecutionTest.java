package com.nhl.link.move;

import com.nhl.link.move.extractor.model.ExtractorName;
import com.nhl.link.move.log.LmLogger;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

public class ExecutionTest {

    @Test
    public void testToString() {
        Execution execution = new Execution(1, "xsync", ExtractorName.create("l", "n"), Map.of("a", 5), mock(LmLogger.class));
        assertEquals("{\"id\":1,\"extractor\":\"l.n\",\"parameters\":{\"a\":5}," +
                "\"stats\":{\"batches\":0,\"created\":0,\"deleted\":0,\"extracted\":0," +
                "\"startedOn\":\"" + execution.getStats().getStartedOn() + "\",\"status\":\"in progress\"," +
                "\"updated\":0},\"task\":\"xsync\"}", execution.toString());

        execution.getStats().incrementCreated(5);
        execution.getStats().incrementDeleted(4);
        execution.getStats().incrementExtracted(55);
        execution.getStats().incrementUpdated(3);
        execution.getStats().incrementSegments(1);

        execution.stop();

        assertEquals("{\"id\":1,\"extractor\":\"l.n\",\"parameters\":{\"a\":5}," +
                "\"stats\":{\"batches\":1,\"created\":5,\"deleted\":4," +
                "\"duration\":\"" + execution.getStats().getDuration() + "\",\"extracted\":55," +
                "\"startedOn\":\"" + execution.getStats().getStartedOn() + "\",\"status\":\"finished\"," +
                "\"updated\":3},\"task\":\"xsync\"}", execution.toString());
    }

    @Deprecated(since = "3.0")
    @Test
    public void testCreateReport() {

        Execution execution = new Execution(1, "xsync", ExtractorName.create("l", "n"), Map.of("a", 5), mock(LmLogger.class));

        assertEquals(Map.of(
                        "Task", "xsync:l.n",
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

        execution.stop();

        assertEquals(Map.of(
                        "Task", "xsync:l.n",
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
        Execution execution = new Execution(1, "xsync", ExtractorName.create("l", "n"), Map.of(), mock(LmLogger.class));

        assertNull(execution.getAttribute("a"));

        execution.setAttribute("a", "MMM");
        assertEquals("MMM", execution.getAttribute("a"));

        execution.setAttribute("a", null);
        assertNull(execution.getAttribute("a"));
    }
}
