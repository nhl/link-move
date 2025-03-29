package com.nhl.link.move.itest;

import com.nhl.link.move.CreateOrUpdateBuilder;
import com.nhl.link.move.Execution;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.runtime.task.createorupdate.CreateOrUpdateStage;
import com.nhl.link.move.unit.LmIntegrationTest;
import com.nhl.link.move.unit.cayenne.t.Etl5t;
import org.junit.jupiter.api.Test;

import java.util.EnumMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CreateOrUpdate_CallbacksIT extends LmIntegrationTest {

    @Test
    public void listeners() {

        CreateOrUpdateBuilder builder = lmRuntime.service(ITaskService.class).
                createOrUpdate(Etl5t.class)
                .sourceExtractor("com/nhl/link/move/itest/etl5_to_etl5t_byid.xml");

        CallbackChecker callbacks = new CallbackChecker().registerAll(builder);

        srcEtl5().insertColumns("id", "name").values(45, "a").values(11, "b").exec();

        Execution e1 = builder.task().run();
        assertExec(2, 2, 0, 0, e1);

        callbacks.assertAllCalled();
    }

    @Test
    public void callbacks() {

        CreateOrUpdateBuilder builder = lmRuntime.service(ITaskService.class).
                createOrUpdate(Etl5t.class)
                .sourceExtractor("com/nhl/link/move/itest/etl5_to_etl5t_byid.xml");

        CallbackChecker callbacks = new CallbackChecker().registerAll(builder);

        srcEtl5().insertColumns("id", "name").values(45, "a").values(11, "b").exec();

        Execution e1 = builder.task().run();
        assertExec(2, 2, 0, 0, e1);

        callbacks.assertAllCalled();
    }

    static class CallbackChecker {
        private final EnumMap<CreateOrUpdateStage, Integer> callbacks;

        public CallbackChecker() {
            callbacks = new EnumMap<>(CreateOrUpdateStage.class);
        }

        public CallbackChecker registerAll(CreateOrUpdateBuilder builder) {
            for (CreateOrUpdateStage stage : CreateOrUpdateStage.values()) {
                register(builder, stage);
            }

            return this;
        }

        private void register(CreateOrUpdateBuilder builder, CreateOrUpdateStage stage) {
            builder.stage(stage, (e, s) -> callbacks.put(stage, callbacks.getOrDefault(stage, 0) + 1));
        }

        public void assertAllCalled() {
            for (CreateOrUpdateStage stage : CreateOrUpdateStage.values()) {
                assertEquals(1, callbacks.get(stage));
            }
        }
    }
}
