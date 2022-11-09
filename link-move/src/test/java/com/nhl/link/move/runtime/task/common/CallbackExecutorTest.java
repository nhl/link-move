package com.nhl.link.move.runtime.task.common;

import com.nhl.link.move.Execution;
import com.nhl.link.move.extractor.model.ExtractorName;
import com.nhl.link.move.log.LmLogger;
import com.nhl.link.move.runtime.task.StageListener;
import com.nhl.link.move.runtime.task.createorupdate.CreateOrUpdateSegment;
import com.nhl.link.move.runtime.task.createorupdate.CreateOrUpdateStage;
import org.junit.jupiter.api.Test;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

class CallbackExecutorTest {

    @Test
    public void testExecuteLambdaCallback() {
        CreateOrUpdateStage expectedStage = CreateOrUpdateStage.MATCH_TARGET;
        CreateOrUpdateStage unexpectedStage = CreateOrUpdateStage.COMMIT_TARGET;
        BiConsumer<Execution, CreateOrUpdateSegment> callback1 = (exec, segment) -> exec.getStats().incrementUpdated(1);
        BiConsumer<Execution, CreateOrUpdateSegment> callback2 = (exec, segment) -> exec.getStats().incrementUpdated(2);
        BiConsumer<Execution, CreateOrUpdateSegment> callback3 = (exec, segment) -> exec.getStats().incrementUpdated(13);

        Map<CreateOrUpdateStage, List<BiConsumer<Execution, CreateOrUpdateSegment>>> callbacks =
                Map.of(
                        expectedStage, List.of(callback1, callback2),
                        unexpectedStage, List.of(callback3)
                );
        CallbackExecutor<CreateOrUpdateStage, CreateOrUpdateSegment> callbackExecutor = new CallbackExecutor<>(Map.of(), callbacks);
        Execution execution = new Execution(1, "xsync", ExtractorName.create("l", "n"), Map.of(), mock(LmLogger.class), null);

        callbackExecutor.executeCallbacks(expectedStage, execution, mock(CreateOrUpdateSegment.class));

        assertEquals(3, execution.getStats().getUpdated());
    }

    @Test
    public void testExecuteAnnotationCallback() {
        CreateOrUpdateStage expectedStage = CreateOrUpdateStage.MATCH_TARGET;
        CreateOrUpdateStage unexpectedStage = CreateOrUpdateStage.COMMIT_TARGET;
        StageListener listener1 = (exec, segment) -> exec.getStats().incrementUpdated(1);
        StageListener listener2 = (exec, segment) -> exec.getStats().incrementUpdated(2);
        StageListener listener3 = (exec, segment) -> exec.getStats().incrementUpdated(13);

        Map<Class<? extends Annotation>, List<StageListener>> annotationCallbacks = Map.of(
                expectedStage.getLegacyAnnotation(), List.of(listener1, listener2),
                unexpectedStage.getLegacyAnnotation(), List.of(listener3)
        );
        CallbackExecutor<CreateOrUpdateStage, CreateOrUpdateSegment> callbackExecutor = new CallbackExecutor<>(annotationCallbacks, Map.of());
        Execution execution = new Execution(1, "xsync", ExtractorName.create("l", "n"), Map.of(), mock(LmLogger.class), null);

        callbackExecutor.executeCallbacks(expectedStage, execution, mock(CreateOrUpdateSegment.class));

        assertEquals(3, execution.getStats().getUpdated());
    }

    @Test
    public void testExecuteLambdaCallbackAndAnnotationCallback() {
        CreateOrUpdateStage stage = CreateOrUpdateStage.MATCH_TARGET;
        StageListener annotationListener = (exec, segment) -> exec.getStats().incrementUpdated(2);
        BiConsumer<Execution, CreateOrUpdateSegment> callback = (exec, segment) -> exec.getStats().incrementUpdated(3);


        Map<Class<? extends Annotation>, List<StageListener>> annotationCallbacks = Map.of(
                stage.getLegacyAnnotation(), List.of(annotationListener)
        );

        Map<CreateOrUpdateStage, List<BiConsumer<Execution, CreateOrUpdateSegment>>> lambdaCallbacks = Map.of(
                stage, List.of(callback)
        );

        Execution execution = new Execution(1, "xsync", ExtractorName.create("l", "n"), Map.of(), mock(LmLogger.class), null);

        CallbackExecutor<CreateOrUpdateStage, CreateOrUpdateSegment> callbackExecutor = new CallbackExecutor<>(annotationCallbacks, lambdaCallbacks);
        callbackExecutor.executeCallbacks(stage, execution, mock(CreateOrUpdateSegment.class));

        assertEquals(5, execution.getStats().getUpdated());
    }
}
