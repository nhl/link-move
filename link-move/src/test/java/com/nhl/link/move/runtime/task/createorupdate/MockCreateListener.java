package com.nhl.link.move.runtime.task.createorupdate;

import com.nhl.link.move.Execution;
import com.nhl.link.move.annotation.*;
import com.nhl.link.move.runtime.task.create.CreateSegment;
import org.mockito.Mockito;

import static org.mockito.Mockito.mock;

public class MockCreateListener {

	private MockCreateListener mockDelegate = mock(MockCreateListener.class);

	public MockCreateListener verify(int times) {
		return Mockito.verify(mockDelegate, Mockito.times(times));
	}

	@AfterSourceRowsExtracted
	public void afterSourceRowsExtracted(Execution execution, CreateSegment<?> segment) {
		mockDelegate.afterSourceRowsExtracted(execution, segment);
	}

	@AfterFksResolved
	public void afterFksResolved(Execution execution, CreateSegment<?> segment) {
		mockDelegate.afterFksResolved(execution, segment);
	}

	@AfterTargetsMatched
	public void afterTargetMatched(Execution execution, CreateSegment<?> segment) {
		mockDelegate.afterTargetMatched(execution, segment);
	}
	
	@AfterTargetsMatched
	public void afterTargetMatched2(Execution execution, CreateSegment<?> segment) {
		mockDelegate.afterTargetMatched2(execution, segment);
	}

	@AfterTargetsMapped
	public void afterTargetMapped(Execution execution, CreateSegment<?> segment) {
		mockDelegate.afterTargetMapped(execution, segment);
	}

	@AfterSourceRowsConverted
	public void afterSourceRowsConverted(Execution execution, CreateSegment<?> segment) {
		mockDelegate.afterSourceRowsConverted(execution, segment);
	}
	
	@AfterSourcesMapped
	public void afterSourceMapped(Execution execution, CreateSegment<?> segment) {
		mockDelegate.afterSourceMapped(execution, segment);
	}
	
	@AfterTargetsMerged
	public void afterTargetMerged(Execution execution, CreateSegment<?> segment) {
		mockDelegate.afterTargetMerged(execution, segment);
	}

	@AfterTargetsCommitted
	public void afterTargetCommited(Execution execution, CreateSegment<?> segment) {
		mockDelegate.afterTargetCommited(execution, segment);
	}
}
