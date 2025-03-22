package com.nhl.link.move.runtime.task;

import com.nhl.link.move.Execution;
import com.nhl.link.move.runtime.task.createorupdate.CreateOrUpdateSegment;
import com.nhl.link.move.runtime.task.createorupdate.CreateOrUpdateStage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.EnumMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ListenersBuilderCallbackTest {

    private ListenersBuilder<CreateOrUpdateSegment, CreateOrUpdateStage> builder;
    private CallbackChecker callbacks;
    private CreateOrUpdateSegment segment;
    private Execution execution;

    @BeforeEach
    public void before() {
        this.builder = new ListenersBuilder<>();
        this.callbacks = new CallbackChecker()
                .registerAll(builder)

                // add the second callback for one of the stages
                .register(builder, CreateOrUpdateStage.MATCH_TARGET);

        this.segment = Mockito.mock(CreateOrUpdateSegment.class);
        this.execution = Mockito.mock(Execution.class);
    }

    @Test
    public void testAfterSourceRowsExtracted() {
        builder.getCallbackExecutor().executeCallbacks(CreateOrUpdateStage.EXTRACT_SOURCE_ROWS, execution, segment);
        callbacks.assertCalled(CreateOrUpdateStage.EXTRACT_SOURCE_ROWS, 1);
    }

    @Test
    public void testAfterSourceRowsConverted() {
        builder.getCallbackExecutor().executeCallbacks(CreateOrUpdateStage.CONVERT_SOURCE_ROWS, execution, segment);
        callbacks.assertCalled(CreateOrUpdateStage.CONVERT_SOURCE_ROWS, 1);
    }

    @Test
    public void testAfterTargetMatched() {
        builder.getCallbackExecutor().executeCallbacks(CreateOrUpdateStage.MATCH_TARGET, execution, segment);
        callbacks.assertCalled(CreateOrUpdateStage.MATCH_TARGET, 2);
    }

    @Test
    public void testAfterTargetsMerged() {
        builder.getCallbackExecutor().executeCallbacks(CreateOrUpdateStage.MERGE_TARGET, execution, segment);
        callbacks.assertCalled(CreateOrUpdateStage.MERGE_TARGET, 1);
    }

    @Test
    public void testAfterTargetsCommitted() {
        builder.getCallbackExecutor().executeCallbacks(CreateOrUpdateStage.COMMIT_TARGET, execution, segment);
        callbacks.assertCalled(CreateOrUpdateStage.COMMIT_TARGET, 1);
    }

    @Test
    public void testAfterSourcesMapped() {
        builder.getCallbackExecutor().executeCallbacks(CreateOrUpdateStage.MAP_SOURCE, execution, segment);
        callbacks.assertCalled(CreateOrUpdateStage.MAP_SOURCE, 1);
    }

    @Test
    public void testAfterFksResolve() {
        builder.getCallbackExecutor().executeCallbacks(CreateOrUpdateStage.RESOLVE_FK_VALUES, execution, segment);
        callbacks.assertCalled(CreateOrUpdateStage.RESOLVE_FK_VALUES, 1);
    }

    static class CallbackChecker {
        private final EnumMap<CreateOrUpdateStage, Integer> callbacks;

        public CallbackChecker() {
            callbacks = new EnumMap<>(CreateOrUpdateStage.class);
        }

        public CallbackChecker registerAll(ListenersBuilder builder) {
            for (CreateOrUpdateStage stage : CreateOrUpdateStage.values()) {
                register(builder, stage);
            }

            return this;
        }

        public CallbackChecker register(ListenersBuilder builder, CreateOrUpdateStage stage) {
            builder.addStageCallback(stage, (e, s) -> callbacks.put(stage, callbacks.getOrDefault(stage, 0) + 1));
            return this;
        }

        public void assertCalled(CreateOrUpdateStage stage, int times) {
            assertEquals(1, callbacks.size());
            assertEquals(times, callbacks.get(stage));
        }
    }
}
