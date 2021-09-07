package com.nhl.link.move.runtime.task;

import com.nhl.link.move.Execution;
import com.nhl.link.move.annotation.*;
import com.nhl.link.move.runtime.task.createorupdate.CreateOrUpdateSegment;
import com.nhl.link.move.runtime.task.createorupdate.MockCreateOrUpdateListener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Matchers;

import java.lang.annotation.Annotation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class ListenersBuilderTest {

    private ListenersBuilder builder;
    private MockCreateOrUpdateListener listener;

    @BeforeEach
    public void before() {
        this.listener = new MockCreateOrUpdateListener();
        this.builder = new ListenersBuilder(
                AfterSourceRowsExtracted.class,
                AfterSourceRowsConverted.class,
                AfterSourcesMapped.class,
                AfterTargetsMatched.class,
                AfterFksResolved.class,
                AfterTargetsMerged.class,
                AfterTargetsCommitted.class).addListener(listener);

        assertEquals(7, builder.getListeners().size());
    }

    private Execution execMatcher() {
        return Matchers.any(Execution.class);
    }

    private CreateOrUpdateSegment segmentMatcher() {
        return Matchers.any(CreateOrUpdateSegment.class);
    }

    private void invoke(Class<? extends Annotation> callbackType) {
        builder.getListeners().get(callbackType).forEach(l ->
                l.afterStageFinished(mock(Execution.class), mock(CreateOrUpdateSegment.class)));
    }

    @Test
    public void testAfterSourceRowsExtracted() {
        assertEquals(1, builder.getListeners().get(AfterSourceRowsExtracted.class).size());

        listener.verify(0).afterSourceRowsExtracted(execMatcher(), segmentMatcher());
        invoke(AfterSourceRowsExtracted.class);
        listener.verify(1).afterSourceRowsExtracted(execMatcher(), segmentMatcher());
    }

    @Test
    public void testAfterSourceRowsConverted() {
        assertEquals(1, builder.getListeners().get(AfterSourceRowsConverted.class).size());

        listener.verify(0).afterSourceRowsConverted(execMatcher(), segmentMatcher());
        invoke(AfterSourceRowsConverted.class);
        listener.verify(1).afterSourceRowsConverted(execMatcher(), segmentMatcher());
    }

    @Test
    public void testAfterTargetMatched() {
        assertEquals(2, builder.getListeners().get(AfterTargetsMatched.class).size());

        listener.verify(0).afterTargetMatched(execMatcher(), segmentMatcher());
        listener.verify(0).afterTargetMatched2(execMatcher(), segmentMatcher());

        invoke(AfterTargetsMatched.class);
        listener.verify(1).afterTargetMatched(execMatcher(), segmentMatcher());
        listener.verify(1).afterTargetMatched2(execMatcher(), segmentMatcher());
    }

    @Test
    public void testAfterTargetsMerged() {
        assertEquals(1, builder.getListeners().get(AfterTargetsMerged.class).size());

        listener.verify(0).afterTargetMerged(execMatcher(), segmentMatcher());
        invoke(AfterTargetsMerged.class);
        listener.verify(1).afterTargetMerged(execMatcher(), segmentMatcher());
    }

    @Test
    public void testAfterTargetsCommitted() {
        assertEquals(1, builder.getListeners().get(AfterTargetsCommitted.class).size());

        listener.verify(0).afterTargetCommited(execMatcher(), segmentMatcher());
        invoke(AfterTargetsCommitted.class);
        listener.verify(1).afterTargetCommited(execMatcher(), segmentMatcher());
    }

    @Test
    public void testAfterSourcesMapped() {
        assertEquals(1, builder.getListeners().get(AfterSourcesMapped.class).size());

        listener.verify(0).afterSourceMapped(execMatcher(), segmentMatcher());
        invoke(AfterSourcesMapped.class);
        listener.verify(1).afterSourceMapped(execMatcher(), segmentMatcher());
    }

    @Test
    public void testAfterFksResolve() {
        assertEquals(1, builder.getListeners().get(AfterFksResolved.class).size());

        listener.verify(0).afterFksResolved(execMatcher(), segmentMatcher());
        invoke(AfterFksResolved.class);
        listener.verify(1).afterFksResolved(execMatcher(), segmentMatcher());
    }
}
