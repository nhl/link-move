package com.nhl.link.move.batch;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class BatchRunnerTest {

	@Test
	public void run_Empty() {

		List<Object> empty = new ArrayList<Object>();

		BatchRunner.create(batch -> fail("Unexpected")).withBatchSize(5).run(empty);
	}

	@Test
	public void run_SingleSmallBatch() {

		List<String> list = Arrays.asList("o1", "o2");
		final int[] batches = new int[1];

		BatchRunner.create((BatchProcessor<String>) batch -> {
			if (batches[0] == 0) {
				batches[0]++;
				assertEquals(Arrays.asList("o1", "o2"), batch);
			} else {
				fail("Unexpected");
			}
		}).withBatchSize(5).run(list);

		assertEquals(1, batches[0]);
	}

	@Test
	public void run_SingleExactBatch() {

		List<String> list = Arrays.asList("o1", "o2", "o3", "o4", "o5");
		final int[] batches = new int[1];

		BatchRunner.create((BatchProcessor<String>) batch -> {
			if (batches[0] == 0) {
				batches[0]++;
				assertEquals(Arrays.asList("o1", "o2", "o3", "o4", "o5"), batch);
			} else {
				fail("Unexpected");
			}
		}).withBatchSize(5).run(list);

		assertEquals(1, batches[0]);
	}

	@Test
	public void run_TwoInexactBatches() {

		List<String> list = Arrays.asList("o1", "o2", "o3", "o4", "o5", "o6");
		final int[] batches = new int[1];

		BatchRunner.create((BatchProcessor<String>) batch -> {
			if (batches[0] == 0) {
				batches[0]++;
				assertEquals(Arrays.asList("o1", "o2", "o3", "o4", "o5"), batch);
			} else if (batches[0] == 1) {
				batches[0]++;
				assertEquals(Arrays.asList("o6"), batch);
			} else {
				fail("Unexpected");
			}
		}).withBatchSize(5).run(list);

		assertEquals(2, batches[0]);
	}

}
