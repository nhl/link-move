package com.nhl.link.etl.runtime.listener;

import com.nhl.link.etl.CreateOrUpdateSegment;
import com.nhl.link.etl.Execution;

/**
 * A wrapper of an annotated listener method.
 * 
 * @since 1.3
 */
public interface CreateOrUpdateListener {

	void afterStageFinished(Execution exec, CreateOrUpdateSegment<?> segment);
}
