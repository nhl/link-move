package com.nhl.link.move.runtime.task.createorupdate;

import com.nhl.link.move.Execution;
import com.nhl.link.move.annotation.*;
import org.mockito.Mockito;

import static org.mockito.Mockito.mock;

public class MockCreateOrUpdateListener {

	private MockCreateOrUpdateListener mockDelegate = mock(MockCreateOrUpdateListener.class);

	public MockCreateOrUpdateListener verify(int times) {
		return Mockito.verify(mockDelegate, Mockito.times(times));
	}

	@AfterSourceRowsExtracted
	public void afterSourceRowsExtracted(Execution execution, CreateOrUpdateSegment<?> segment) {
		mockDelegate.afterSourceRowsExtracted(execution, segment);
	}

	@AfterFksResolved
	public void afterFksResolved(Execution execution, CreateOrUpdateSegment<?> segment) {
		mockDelegate.afterFksResolved(execution, segment);
	}

	@AfterTargetsMatched
	public void afterTargetMatched(Execution execution, CreateOrUpdateSegment<?> segment) {
		mockDelegate.afterTargetMatched(execution, segment);
	}
	
	@AfterTargetsMatched
	public void afterTargetMatched2(Execution execution, CreateOrUpdateSegment<?> segment) {
		mockDelegate.afterTargetMatched2(execution, segment);
	}

	@AfterSourceRowsConverted
	public void afterSourceRowsConverted(Execution execution, CreateOrUpdateSegment<?> segment) {
		mockDelegate.afterSourceRowsConverted(execution, segment);
	}
	
	@AfterSourcesMapped
	public void afterSourceMapped(Execution execution, CreateOrUpdateSegment<?> segment) {
		mockDelegate.afterSourceMapped(execution, segment);
	}
	
	@AfterTargetsMerged
	public void afterTargetMerged(Execution execution, CreateOrUpdateSegment<?> segment) {
		mockDelegate.afterTargetMerged(execution, segment);
	}

	@AfterTargetsCommitted
	public void afterTargetCommited(Execution execution, CreateOrUpdateSegment<?> segment) {
		mockDelegate.afterTargetCommited(execution, segment);
	}

	public MockCreateOrUpdateListener getMockDelegate() {
		return mockDelegate;
	}
}
