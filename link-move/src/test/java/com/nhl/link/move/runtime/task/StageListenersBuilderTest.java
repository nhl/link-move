package com.nhl.link.move.runtime.task;

import com.nhl.link.move.Execution;
import com.nhl.link.move.annotation.*;
import com.nhl.link.move.runtime.task.createorupdate.CreateOrUpdateSegment;
import com.nhl.link.move.runtime.task.createorupdate.MockCreateOrUpdateListener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Matchers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class StageListenersBuilderTest {

	private ListenersBuilder listenersBuilder;

	@BeforeEach
	public void before() {
		listenersBuilder = new ListenersBuilder(AfterSourceRowsConverted.class, AfterSourcesMapped.class,
				AfterTargetsMatched.class, AfterTargetsMerged.class, AfterTargetsCommitted.class);
	}

	@Test
	public void testAddListener() {

		MockCreateOrUpdateListener listener1 = new MockCreateOrUpdateListener();
		listenersBuilder.addListener(listener1);

		assertEquals(5, listenersBuilder.getListeners().size());
		assertEquals(2, listenersBuilder.getListeners().get(AfterTargetsMatched.class).size());
		assertEquals(1, listenersBuilder.getListeners().get(AfterSourceRowsConverted.class).size());
		assertEquals(1, listenersBuilder.getListeners().get(AfterSourcesMapped.class).size());
		assertEquals(1, listenersBuilder.getListeners().get(AfterTargetsMerged.class).size());
		assertEquals(1, listenersBuilder.getListeners().get(AfterTargetsCommitted.class).size());

		verify(listener1.getMockDelegate(), times(0)).afterSourceRowsConverted(Matchers.any(Execution.class),
				Matchers.any(CreateOrUpdateSegment.class));
		listenersBuilder.getListeners().get(AfterSourceRowsConverted.class).get(0)
				.afterStageFinished(mock(Execution.class), mock(CreateOrUpdateSegment.class));
		verify(listener1.getMockDelegate()).afterSourceRowsConverted(Matchers.any(Execution.class),
				Matchers.any(CreateOrUpdateSegment.class));

		verify(listener1.getMockDelegate(), times(0)).afterTargetMatched(Matchers.any(Execution.class),
				Matchers.any(CreateOrUpdateSegment.class));
		verify(listener1.getMockDelegate(), times(0)).afterTargetMatched2(Matchers.any(Execution.class),
				Matchers.any(CreateOrUpdateSegment.class));
		listenersBuilder.getListeners().get(AfterTargetsMatched.class).get(0)
				.afterStageFinished(mock(Execution.class), mock(CreateOrUpdateSegment.class));
		listenersBuilder.getListeners().get(AfterTargetsMatched.class).get(1)
				.afterStageFinished(mock(Execution.class), mock(CreateOrUpdateSegment.class));
		verify(listener1.getMockDelegate()).afterTargetMatched(Matchers.any(Execution.class),
				Matchers.any(CreateOrUpdateSegment.class));
		verify(listener1.getMockDelegate()).afterTargetMatched2(Matchers.any(Execution.class),
				Matchers.any(CreateOrUpdateSegment.class));

		verify(listener1.getMockDelegate(), times(0)).afterTargetCommited(Matchers.any(Execution.class),
				Matchers.any(CreateOrUpdateSegment.class));
		listenersBuilder.getListeners().get(AfterTargetsCommitted.class).get(0)
				.afterStageFinished(mock(Execution.class), mock(CreateOrUpdateSegment.class));

	}
}
