package com.nhl.link.framework.etl.batch;

import java.util.List;

/**
 * A callback for the {@link BatchRunner}.
 */
public interface BatchProcessor<T> {

	void process(List<T> segment);
}
