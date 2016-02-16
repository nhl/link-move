package com.nhl.link.move.runtime.task.createorupdate;

import static org.mockito.Mockito.mock;

import com.nhl.link.move.Execution;
import com.nhl.link.move.annotation.AfterSourceRowsConverted;
import com.nhl.link.move.annotation.AfterSourcesMapped;
import com.nhl.link.move.annotation.AfterTargetsMatched;
import com.nhl.link.move.annotation.AfterTargetsMerged;
import com.nhl.link.move.runtime.task.createorupdate.CreateOrUpdateSegment;

public class MockCreateOrUpdateListener {

	private MockCreateOrUpdateListener mockDelegate = mock(MockCreateOrUpdateListener.class);

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

	public MockCreateOrUpdateListener getMockDelegate() {
		return mockDelegate;
	}
}
