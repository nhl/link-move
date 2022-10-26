package com.nhl.link.move.json.unit;

import com.nhl.link.move.Execution;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A helper class to test the results of the ETL task run.
 */
public class LmTaskTester {

    private int expectedExtracted;
    private int expectedCreated;
    private int expectedUpdated;
    private int expectedDeleted;

    public LmTaskTester() {
        expectedCreated = -1;
        expectedExtracted = -1;
        expectedUpdated = -1;
        expectedDeleted = -1;
    }

    public LmTaskTester shouldExtract(int expectedExtracted) {
        this.expectedExtracted = expectedExtracted;
        return this;
    }

    public LmTaskTester shouldCreate(int expectedCreated) {
        this.expectedCreated = expectedCreated;
        return this;
    }

    public LmTaskTester shouldUpdate(int expectedUpdated) {
        this.expectedUpdated = expectedUpdated;
        return this;
    }

    public LmTaskTester shouldDelete(int expectedDeleted) {
        this.expectedDeleted = expectedDeleted;
        return this;
    }

    public void test(Execution exec) {

        if (expectedExtracted >= 0) {
            assertEquals(expectedExtracted, exec.getStats().getExtracted(), "Extracted unexpected number of records.");
        }

        if (expectedCreated > 0) {
            assertEquals(expectedCreated, exec.getStats().getCreated(), "Created unexpected number of records.");
        }

        if (expectedUpdated > 0) {
            assertEquals(expectedUpdated, exec.getStats().getUpdated(), "Updated unexpected number of records.");
        }

        if (expectedDeleted > 0) {
            assertEquals(expectedDeleted, exec.getStats().getDeleted(), "Deleted unexpected number of records.");
        }
    }
}
