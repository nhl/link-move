package com.nhl.link.etl.batch;

import java.util.List;

/**
 * A callback for the {@link BatchRunner}.
 */
public interface BatchProcessor<S> {

	void process(List<S> segment);
}
