package com.nhl.link.move.runtime.task.common;

import com.nhl.link.move.Execution;
import com.nhl.link.move.extractor.model.ExtractorName;
import com.nhl.link.move.log.LmLogger;
import com.nhl.link.move.runtime.task.createorupdate.CreateOrUpdateSegment;
import com.nhl.link.move.runtime.task.createorupdate.CreateOrUpdateStage;
import org.junit.jupiter.api.Test;

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
        CallbackExecutor<CreateOrUpdateStage, CreateOrUpdateSegment> callbackExecutor = new CallbackExecutor<>(callbacks);
        Execution execution = new Execution(1, "xsync", ExtractorName.create("l", "n"), Map.of(), mock(LmLogger.class), null);

        callbackExecutor.executeCallbacks(expectedStage, execution, mock(CreateOrUpdateSegment.class));

        assertEquals(3, execution.getStats().getUpdated());
    }
}
