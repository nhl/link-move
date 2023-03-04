package com.nhl.link.move.json.unit;

import com.nhl.link.move.Execution;
import com.nhl.link.move.runtime.LmRuntime;
import com.nhl.link.move.runtime.LmRuntimeBuilder;
import org.junit.jupiter.api.BeforeEach;

public abstract class LmJsonIntegrationTest extends DerbyTargetTest {

    protected LmRuntime lmRuntime;

    @BeforeEach
    protected void initLmRuntime() {
        this.lmRuntime = testRuntimeBuilder().build();
    }

    protected LmRuntimeBuilder testRuntimeBuilder() {
        return LmRuntime.builder().targetRuntime(targetCayenne.getRuntime());
    }

    protected void assertExec(int extracted, int created, int updated, int deleted, Execution exec) {
        new LmTaskTester()
                .shouldExtract(extracted)
                .shouldCreate(created)
                .shouldUpdate(updated)
                .shouldDelete(deleted)
                .test(exec);
    }
}
