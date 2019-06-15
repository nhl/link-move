package com.nhl.link.move.runtime.json.it;

import com.nhl.link.move.Execution;

import static org.junit.Assert.assertEquals;

public abstract class LmItTest {

    protected void assertExec(int extracted, int created, int updated, int deleted, Execution execution) {
        if (extracted >= 0) {
            assertEquals(extracted, execution.getStats().getExtracted());
        }
        if (created > 0) {
            assertEquals(created, execution.getStats().getCreated());
        }
        if (updated > 0) {
            assertEquals(updated, execution.getStats().getUpdated());
        }
        if (deleted > 0) {
            assertEquals(deleted, execution.getStats().getDeleted());
        }
    }
}
