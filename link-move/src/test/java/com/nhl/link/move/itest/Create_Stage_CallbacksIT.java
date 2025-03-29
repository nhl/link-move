package com.nhl.link.move.itest;

import com.nhl.link.move.CreateBuilder;
import com.nhl.link.move.Execution;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.runtime.task.create.CreateStage;
import com.nhl.link.move.unit.LmIntegrationTest;
import com.nhl.link.move.unit.cayenne.t.Etl1t;
import org.junit.jupiter.api.Test;

import java.util.EnumMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class Create_Stage_CallbacksIT extends LmIntegrationTest {

    @Test
    public void callbacks() {

        CreateBuilder builder = lmRuntime.service(ITaskService.class)
                .create(Etl1t.class)
                .sourceExtractor("com/nhl/link/move/itest/etl1_to_etl1t_upper.xml");

        CallbackChecker callbacks = new CallbackChecker().registerAll(builder);

        srcEtl1().insertColumns("name", "age")
                .values("a", 3)
                .values("b", null)
                .exec();

        Execution e1 = builder.task().run();
        assertExec(2, 2, 0, 0, e1);

        callbacks.assertAllCalled();
    }

    static class CallbackChecker {
        private final EnumMap<CreateStage, Integer> callbacks;

        public CallbackChecker() {
            callbacks = new EnumMap<>(CreateStage.class);
        }

        public CallbackChecker registerAll(CreateBuilder builder) {
            for (CreateStage stage : CreateStage.values()) {
                register(builder, stage);
            }

            return this;
        }

        private void register(CreateBuilder builder, CreateStage stage) {
            builder.stage(stage, (e, s) -> callbacks.put(stage, callbacks.getOrDefault(stage, 0) + 1));
        }

        public void assertAllCalled() {
            for (CreateStage stage : CreateStage.values()) {
                assertEquals(1, callbacks.get(stage));
            }
        }
    }
}
