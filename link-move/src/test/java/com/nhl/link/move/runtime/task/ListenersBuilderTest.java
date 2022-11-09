package com.nhl.link.move.runtime.task;

import com.nhl.link.move.Execution;
import com.nhl.link.move.annotation.AfterFksResolved;
import com.nhl.link.move.annotation.AfterMissingTargetsFiltered;
import com.nhl.link.move.annotation.AfterSourceRowsConverted;
import com.nhl.link.move.annotation.AfterSourceRowsExtracted;
import com.nhl.link.move.annotation.AfterSourcesMapped;
import com.nhl.link.move.annotation.AfterTargetsCommitted;
import com.nhl.link.move.annotation.AfterTargetsExtracted;
import com.nhl.link.move.annotation.AfterTargetsMapped;
import com.nhl.link.move.annotation.AfterTargetsMatched;
import com.nhl.link.move.annotation.AfterTargetsMerged;
import com.nhl.link.move.runtime.task.common.CallbackExecutor;
import com.nhl.link.move.runtime.task.createorupdate.CreateOrUpdateSegment;
import com.nhl.link.move.runtime.task.createorupdate.CreateOrUpdateStage;
import com.nhl.link.move.runtime.task.createorupdate.MockCreateOrUpdateListener;
import com.nhl.link.move.runtime.task.delete.DeleteSegment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ListenersBuilderTest {

    private ListenersBuilder<CreateOrUpdateSegment, CreateOrUpdateStage> builder;
    private MockCreateOrUpdateListener listener;
    private Execution execution = Mockito.mock(Execution.class);
    private CreateOrUpdateSegment segment = Mockito.mock(CreateOrUpdateSegment.class);

    @BeforeEach
    public void before() {
        this.listener = new MockCreateOrUpdateListener();
        this.builder = new ListenersBuilder<CreateOrUpdateSegment, CreateOrUpdateStage>(
                AfterSourceRowsExtracted.class,
                AfterSourceRowsConverted.class,
                AfterSourcesMapped.class,
                AfterTargetsMatched.class,
                AfterFksResolved.class,
                AfterTargetsMerged.class,
                AfterTargetsCommitted.class).addListener(listener);

        assertEquals(7, builder.getListeners().size());
    }
    
    @AfterEach
    public void afterEach() {
        Mockito.verifyNoMoreInteractions(execution, segment);
        listener.verifyNoMoreInteractions();
        Mockito.reset(execution, segment);
    }

    @Test
    public void testAfterSourceRowsExtracted() {
        CallbackExecutor<CreateOrUpdateStage, CreateOrUpdateSegment> callbackExecutor = builder.getCallbackExecutor();
        
        callbackExecutor.executeCallbacks(CreateOrUpdateStage.EXTRACT_SOURCE_ROWS, execution, segment);
        
        listener.verify(1).afterSourceRowsExtracted(Matchers.eq(execution), Matchers.eq(segment));
    }

    @Test
    public void testAfterSourceRowsConverted() {
        CallbackExecutor<CreateOrUpdateStage, CreateOrUpdateSegment> callbackExecutor = builder.getCallbackExecutor();

        callbackExecutor.executeCallbacks(CreateOrUpdateStage.CONVERT_SOURCE_ROWS, execution, segment);

        listener.verify(1).afterSourceRowsConverted(eq(execution), eq(segment));
    }

    @Test
    public void testAfterTargetMatched() {
        CallbackExecutor<CreateOrUpdateStage, CreateOrUpdateSegment> callbackExecutor = builder.getCallbackExecutor();

        callbackExecutor.executeCallbacks(CreateOrUpdateStage.MATCH_TARGET, execution, segment);

        listener.verify(1).afterTargetMatched(eq(execution), eq(segment));
        listener.verify(1).afterTargetMatched2(eq(execution), eq(segment));
    }

    @Test
    public void testAfterTargetsMerged() {
        CallbackExecutor<CreateOrUpdateStage, CreateOrUpdateSegment> callbackExecutor = builder.getCallbackExecutor();

        callbackExecutor.executeCallbacks(CreateOrUpdateStage.MERGE_TARGET, execution, segment);
        
        listener.verify(1).afterTargetMerged(eq(execution), eq(segment));
    }

    @Test
    public void testAfterTargetsCommitted() {
        CallbackExecutor<CreateOrUpdateStage, CreateOrUpdateSegment> callbackExecutor = builder.getCallbackExecutor();

        callbackExecutor.executeCallbacks(CreateOrUpdateStage.COMMIT_TARGET, execution, segment);

        listener.verify(1).afterTargetCommited(eq(execution), eq(segment));
    }

    @Test
    public void testAfterSourcesMapped() {
        CallbackExecutor<CreateOrUpdateStage, CreateOrUpdateSegment> callbackExecutor = builder.getCallbackExecutor();

        callbackExecutor.executeCallbacks(CreateOrUpdateStage.MAP_SOURCE, execution, segment);

        listener.verify(1).afterSourceMapped(eq(execution), eq(segment));
    }

    @Test
    public void testAfterFksResolve() {
        CallbackExecutor<CreateOrUpdateStage, CreateOrUpdateSegment> callbackExecutor = builder.getCallbackExecutor();

        callbackExecutor.executeCallbacks(CreateOrUpdateStage.RESOLVE_FK_VALUES, execution, segment);

        listener.verify(1).afterFksResolved(eq(execution), eq(segment));
    }

    @Test
    public void testListenerWithCallback() {
        MockCreateOrUpdateCallback extractSourceRowsCallback = Mockito.mock(MockCreateOrUpdateCallback.class);

        builder.addStageCallback(CreateOrUpdateStage.EXTRACT_SOURCE_ROWS, extractSourceRowsCallback);
        CallbackExecutor<CreateOrUpdateStage, CreateOrUpdateSegment> callbackExecutor = builder.getCallbackExecutor();

        callbackExecutor.executeCallbacks(CreateOrUpdateStage.EXTRACT_SOURCE_ROWS, execution, segment);

        verify(extractSourceRowsCallback, times(1)).accept(execution, segment);
        listener.verify(1).afterSourceRowsExtracted(Matchers.eq(execution), Matchers.eq(segment));
    }

    @Test
    public void testAddListener() {
        ListenersBuilder listenersBuilder = new ListenersBuilder(
                AfterTargetsExtracted.class,
                AfterSourceRowsExtracted.class,
                AfterTargetsMapped.class,
                AfterMissingTargetsFiltered.class,
                AfterTargetsCommitted.class);

        DeleteListener1 l1 = new DeleteListener1();
        DeleteListener2 l2 = new DeleteListener2();
        NotAListener l3 = new NotAListener();

        listenersBuilder.addListener(l1);
        listenersBuilder.addListener(l2);
        listenersBuilder.addListener(l3);

        Map<Class<? extends Annotation>, List<StageListener>> listeners = listenersBuilder.getListeners();

        assertNotNull(listeners.get(AfterTargetsMapped.class));
        assertNotNull(listeners.get(AfterTargetsExtracted.class));
        assertNotNull(listeners.get(AfterMissingTargetsFiltered.class));

        assertEquals(1, listeners.get(AfterTargetsMapped.class).size());
        assertEquals(2, listeners.get(AfterTargetsExtracted.class).size());
        assertEquals(1, listeners.get(AfterMissingTargetsFiltered.class).size());
    }

    public static class DeleteListener1 {

        @AfterTargetsMapped
        public void afterTargetsMapped(DeleteSegment s) {

        }

        @AfterTargetsExtracted
        public void afterSourceKeysExtracted(DeleteSegment s) {

        }
    }

    public static class DeleteListener2 {

        @AfterMissingTargetsFiltered
        public void afterMissingTargetsFiltered(DeleteSegment s) {

        }

        @AfterTargetsExtracted
        public void afterSourceKeysExtracted(DeleteSegment s) {

        }
    }

    public static class NotAListener {

        public void someMethod(DeleteSegment s) {

        }
    }
}
