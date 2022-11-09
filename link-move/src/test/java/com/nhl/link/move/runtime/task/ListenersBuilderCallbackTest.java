package com.nhl.link.move.runtime.task;

import com.nhl.link.move.Execution;
import com.nhl.link.move.annotation.*;
import com.nhl.link.move.runtime.task.common.CallbackExecutor;
import com.nhl.link.move.runtime.task.createorupdate.CreateOrUpdateSegment;
import com.nhl.link.move.runtime.task.createorupdate.CreateOrUpdateStage;
import com.nhl.link.move.runtime.task.createorupdate.MockCreateOrUpdateListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class ListenersBuilderCallbackTest {

    private ListenersBuilder<CreateOrUpdateSegment, CreateOrUpdateStage> builder;
    private CreateOrUpdateSegment segment = Mockito.mock(CreateOrUpdateSegment.class);
    private Execution execution = Mockito.mock(Execution.class);

    private MockCreateOrUpdateCallback extractSourceRowsCallback = Mockito.mock(MockCreateOrUpdateCallback.class);
    private MockCreateOrUpdateCallback convertSourceRowsCallback = Mockito.mock(MockCreateOrUpdateCallback.class);
    private MockCreateOrUpdateCallback mapSourceCallback = Mockito.mock(MockCreateOrUpdateCallback.class);
    private MockCreateOrUpdateCallback matchTargetCallback1 = Mockito.mock(MockCreateOrUpdateCallback.class);
    private MockCreateOrUpdateCallback matchTargetCallback2 = Mockito.mock(MockCreateOrUpdateCallback.class);
    private MockCreateOrUpdateCallback mapTargetCallback = Mockito.mock(MockCreateOrUpdateCallback.class);
    private MockCreateOrUpdateCallback resolveFkCallback = Mockito.mock(MockCreateOrUpdateCallback.class);
    private MockCreateOrUpdateCallback mergeTargetCallback = Mockito.mock(MockCreateOrUpdateCallback.class);
    private MockCreateOrUpdateCallback commitTargetCallback = Mockito.mock(MockCreateOrUpdateCallback.class);

    @BeforeEach
    public void before() {
        this.builder = new ListenersBuilder<>(AfterSourceRowsExtracted.class,
                AfterSourceRowsConverted.class,
                AfterSourcesMapped.class,
                AfterTargetsMatched.class,
                AfterFksResolved.class,
                AfterTargetsMerged.class,
                AfterTargetsCommitted.class);

        builder.addStageCallback(CreateOrUpdateStage.EXTRACT_SOURCE_ROWS, extractSourceRowsCallback);
        builder.addStageCallback(CreateOrUpdateStage.CONVERT_SOURCE_ROWS, convertSourceRowsCallback);
        builder.addStageCallback(CreateOrUpdateStage.MAP_SOURCE, mapSourceCallback);
        builder.addStageCallback(CreateOrUpdateStage.MATCH_TARGET, matchTargetCallback1);
        builder.addStageCallback(CreateOrUpdateStage.MATCH_TARGET, matchTargetCallback2);
        builder.addStageCallback(CreateOrUpdateStage.MAP_TARGET, mapTargetCallback);
        builder.addStageCallback(CreateOrUpdateStage.RESOLVE_FK_VALUES, resolveFkCallback);
        builder.addStageCallback(CreateOrUpdateStage.MERGE_TARGET, mergeTargetCallback);
        builder.addStageCallback(CreateOrUpdateStage.COMMIT_TARGET, commitTargetCallback);
    }

    @AfterEach
    public void afterEach() {
        Mockito.reset(execution, segment);
        Mockito.verifyNoMoreInteractions(
                execution,
                segment,
                extractSourceRowsCallback,
                convertSourceRowsCallback,
                mapSourceCallback,
                matchTargetCallback1,
                matchTargetCallback2,
                mapTargetCallback,
                resolveFkCallback,
                mergeTargetCallback,
                commitTargetCallback
        );
    }

    @Test
    public void testAfterSourceRowsExtracted() {
        CallbackExecutor<CreateOrUpdateStage, CreateOrUpdateSegment> callbackExecutor = builder.getCallbackExecutor();

        callbackExecutor.executeCallbacks(CreateOrUpdateStage.EXTRACT_SOURCE_ROWS, execution, segment);

        verify(extractSourceRowsCallback, times(1)).accept(execution, segment);
    }

    @Test
    public void testAfterSourceRowsConverted() {
        CallbackExecutor<CreateOrUpdateStage, CreateOrUpdateSegment> callbackExecutor = builder.getCallbackExecutor();

        callbackExecutor.executeCallbacks(CreateOrUpdateStage.CONVERT_SOURCE_ROWS, execution, segment);

        verify(convertSourceRowsCallback, times(1)).accept(execution, segment);
    }

    @Test
    public void testAfterTargetMatched() {
        CallbackExecutor<CreateOrUpdateStage, CreateOrUpdateSegment> callbackExecutor = builder.getCallbackExecutor();

        callbackExecutor.executeCallbacks(CreateOrUpdateStage.MATCH_TARGET, execution, segment);

        verify(matchTargetCallback1, times(1)).accept(execution, segment);
        verify(matchTargetCallback2, times(1)).accept(execution, segment);
    }

    @Test
    public void testAfterTargetsMerged() {
        CallbackExecutor<CreateOrUpdateStage, CreateOrUpdateSegment> callbackExecutor = builder.getCallbackExecutor();

        callbackExecutor.executeCallbacks(CreateOrUpdateStage.MERGE_TARGET, execution, segment);

        verify(mergeTargetCallback, times(1)).accept(execution, segment);
    }

    @Test
    public void testAfterTargetsCommitted() {
        CallbackExecutor<CreateOrUpdateStage, CreateOrUpdateSegment> callbackExecutor = builder.getCallbackExecutor();

        callbackExecutor.executeCallbacks(CreateOrUpdateStage.COMMIT_TARGET, execution, segment);

        verify(commitTargetCallback, times(1)).accept(execution, segment);
    }


    @Test
    public void testAfterSourcesMapped() {
        CallbackExecutor<CreateOrUpdateStage, CreateOrUpdateSegment> callbackExecutor = builder.getCallbackExecutor();

        callbackExecutor.executeCallbacks(CreateOrUpdateStage.MAP_SOURCE, execution, segment);

        verify(mapSourceCallback, times(1)).accept(execution, segment);
    }

    @Test
    public void testAfterFksResolve() {
        CallbackExecutor<CreateOrUpdateStage, CreateOrUpdateSegment> callbackExecutor = builder.getCallbackExecutor();

        callbackExecutor.executeCallbacks(CreateOrUpdateStage.RESOLVE_FK_VALUES, execution, segment);

        verify(resolveFkCallback, times(1)).accept(execution, segment);
    }

    @Test
    public void testCallbackWithAnnotationListener() {
        MockCreateOrUpdateListener legacyListener = new MockCreateOrUpdateListener();

        builder.addListener(legacyListener);
        builder.getCallbackExecutor().executeCallbacks(CreateOrUpdateStage.RESOLVE_FK_VALUES, execution, segment);

        verify(resolveFkCallback, times(1)).accept(execution, segment);
        legacyListener.verify(1).afterFksResolved(eq(execution), eq(segment));
        legacyListener.verifyNoMoreInteractions();
    }

    @Test
    public void testNoCallback() {
        builder = new ListenersBuilder<>();

        builder.getCallbackExecutor().executeCallbacks(CreateOrUpdateStage.COMMIT_TARGET, execution, segment);

        verifyNoMoreInteractions(execution, segment);
    }
}
