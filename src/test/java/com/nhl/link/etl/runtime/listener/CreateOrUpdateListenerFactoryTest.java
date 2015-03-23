package com.nhl.link.etl.runtime.listener;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.mockito.Matchers;

import com.nhl.link.etl.CreateOrUpdateSegment;
import com.nhl.link.etl.Execution;
import com.nhl.link.etl.annotation.AfterSourceRowsConverted;
import com.nhl.link.etl.annotation.AfterSourcesMapped;
import com.nhl.link.etl.annotation.AfterTargetMatched;
import com.nhl.link.etl.annotation.AfterTargetMerged;

public class CreateOrUpdateListenerFactoryTest {

	@Test
	public void testAppendListeners() {
		Map<Class<? extends Annotation>, List<CreateOrUpdateListener>> listeners = new HashMap<>();

		MockCreateOrUpdateListener listener1 = new MockCreateOrUpdateListener();
		CreateOrUpdateListenerFactory.appendListeners(listeners, listener1);

		assertEquals(4, listeners.size());
		assertEquals(2, listeners.get(AfterTargetMatched.class).size());
		assertEquals(1, listeners.get(AfterSourceRowsConverted.class).size());
		assertEquals(1, listeners.get(AfterSourcesMapped.class).size());
		assertEquals(1, listeners.get(AfterTargetMerged.class).size());

		verify(listener1.getMockDelegate(), times(0)).afterSourceRowsConverted(Matchers.any(Execution.class),
				Matchers.any(CreateOrUpdateSegment.class));
		listeners.get(AfterSourceRowsConverted.class).get(0)
				.afterStageFinished(mock(Execution.class), mock(CreateOrUpdateSegment.class));
		verify(listener1.getMockDelegate()).afterSourceRowsConverted(Matchers.any(Execution.class),
				Matchers.any(CreateOrUpdateSegment.class));

		verify(listener1.getMockDelegate(), times(0)).afterTargetMatched(Matchers.any(Execution.class),
				Matchers.any(CreateOrUpdateSegment.class));
		verify(listener1.getMockDelegate(), times(0)).afterTargetMatched2(Matchers.any(Execution.class),
				Matchers.any(CreateOrUpdateSegment.class));
		listeners.get(AfterTargetMatched.class).get(0)
				.afterStageFinished(mock(Execution.class), mock(CreateOrUpdateSegment.class));
		listeners.get(AfterTargetMatched.class).get(1)
				.afterStageFinished(mock(Execution.class), mock(CreateOrUpdateSegment.class));
		verify(listener1.getMockDelegate()).afterTargetMatched(Matchers.any(Execution.class),
				Matchers.any(CreateOrUpdateSegment.class));
		verify(listener1.getMockDelegate()).afterTargetMatched2(Matchers.any(Execution.class),
				Matchers.any(CreateOrUpdateSegment.class));

	}
}
