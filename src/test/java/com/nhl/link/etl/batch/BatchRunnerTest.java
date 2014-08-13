package com.nhl.link.etl.batch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.nhl.link.etl.batch.BatchConverter;
import com.nhl.link.etl.batch.BatchProcessor;
import com.nhl.link.etl.batch.BatchRunner;

public class BatchRunnerTest {

	@Test
	public void testRun_Empty() {

		List<Object> empty = new ArrayList<Object>();

		BatchRunner.create(new BatchProcessor<Object>() {

			@Override
			public void process(List<Object> batch) {
				fail("Unexpected");
			}
		}).withBatchSize(5).run(empty);
	}

	@Test
	public void testRun_SingleSmallBatch() {

		List<String> list = Arrays.asList("o1", "o2");
		final int[] batches = new int[1];

		BatchRunner.create(new BatchProcessor<String>() {

			@Override
			public void process(List<String> batch) {
				if (batches[0] == 0) {
					batches[0]++;
					assertEquals(Arrays.asList("o1", "o2"), batch);
				} else {
					fail("Unexpected");
				}
			}
		}).withBatchSize(5).run(list);

		assertEquals(1, batches[0]);
	}

	@Test
	public void testRun_SingleExactBatch() {

		List<String> list = Arrays.asList("o1", "o2", "o3", "o4", "o5");
		final int[] batches = new int[1];

		BatchRunner.create(new BatchProcessor<String>() {

			@Override
			public void process(List<String> batch) {
				if (batches[0] == 0) {
					batches[0]++;
					assertEquals(Arrays.asList("o1", "o2", "o3", "o4", "o5"), batch);
				} else {
					fail("Unexpected");
				}
			}
		}).withBatchSize(5).run(list);

		assertEquals(1, batches[0]);
	}

	@Test
	public void testRun_TwoInexactBatches() {

		List<String> list = Arrays.asList("o1", "o2", "o3", "o4", "o5", "o6");
		final int[] batches = new int[1];

		BatchRunner.create(new BatchProcessor<String>() {

			@Override
			public void process(List<String> batch) {
				if (batches[0] == 0) {
					batches[0]++;
					assertEquals(Arrays.asList("o1", "o2", "o3", "o4", "o5"), batch);
				} else if (batches[0] == 1) {
					batches[0]++;
					assertEquals(Arrays.asList("o6"), batch);
				} else {
					fail("Unexpected");
				}
			}
		}).withBatchSize(5).run(list);

		assertEquals(2, batches[0]);
	}

	@Test
	public void testRun_TwoInexactBatches_Converter() {

		List<String> list = Arrays.asList("o1", "o2", "o3", "o4", "o5", "o6");
		final int[] batches = new int[1];

		BatchConverter<String, String> converter = new BatchConverter<String, String>() {

			private int i;

			@Override
			public String fromTemplate(String source, String target) {
				return target + source;
			}

			@Override
			public String createTemplate() {
				return Integer.toString(i++);
			}
		};

		BatchRunner.create(new BatchProcessor<String>() {

			@Override
			public void process(List<String> batch) {
				if (batches[0] == 0) {
					batches[0]++;
					assertEquals(Arrays.asList("0o1", "1o2", "2o3", "3o4", "4o5"), batch);
				} else if (batches[0] == 1) {
					batches[0]++;
					assertEquals(Arrays.asList("0o6"), batch);
				} else {
					fail("Unexpected");
				}
			}
		}).withBatchSize(5).run(list, converter);

		assertEquals(2, batches[0]);
	}

}
