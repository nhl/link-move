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
        Execution execution = new Execution(1, "xsync", ExtractorName.create("location", "name"), Map.of("a", 5), mock(LmLogger.class), null);
        assertEquals("{\"id\":1,\"extractor\":\"location#name\",\"parameters\":{\"a\":5}," +
                "\"stats\":{\"batches\":0,\"created\":0,\"deleted\":0,\"extracted\":0," +
                "\"startedOn\":\"" + execution.getStats().getStartedOn() + "\",\"status\":\"in progress\"," +
                "\"updated\":0},\"task\":\"xsync\"}", execution.toString());

        execution.getStats().incrementCreated(5);
        execution.getStats().incrementDeleted(4);
        execution.getStats().incrementExtracted(55);
        execution.getStats().incrementUpdated(3);
        execution.getStats().incrementSegments(1);

        execution.stop();

        assertEquals("{\"id\":1,\"extractor\":\"location#name\",\"parameters\":{\"a\":5}," +
                "\"stats\":{\"batches\":1,\"created\":5,\"deleted\":4," +
                "\"duration\":\"" + execution.getStats().getDuration() + "\",\"extracted\":55," +
                "\"startedOn\":\"" + execution.getStats().getStartedOn() + "\",\"status\":\"finished\"," +
                "\"updated\":3},\"task\":\"xsync\"}", execution.toString());
    }

    @Test
    public void attribute() {
        Execution execution = new Execution(1, "xsync", ExtractorName.create("l", "n"), Map.of(), mock(LmLogger.class), null);

        assertNull(execution.getAttribute("a"));

        execution.setAttribute("a", "MMM");
        assertEquals("MMM", execution.getAttribute("a"));

        execution.setAttribute("a", null);
        assertNull(execution.getAttribute("a"));
    }
}
