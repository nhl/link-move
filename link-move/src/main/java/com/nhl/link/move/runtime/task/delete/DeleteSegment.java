package com.nhl.link.move.runtime.task.delete;

import com.nhl.dflib.DataFrame;
import com.nhl.link.move.runtime.task.common.DataSegment;
import org.apache.cayenne.ObjectContext;

import java.util.Set;

public class DeleteSegment implements DataSegment {

	public static final String TARGET_COLUMN = "$lm_target";
	public static final String KEY_COLUMN = "$lm_key";

	private final ObjectContext context;

	private DataFrame targets;
	private Set<Object> sourceKeys;
	private DataFrame mappedTargets;
	private DataFrame missingTargets;

	public DeleteSegment(ObjectContext context) {
		this.context = context;
	}

	public ObjectContext getContext() {
		return context;
	}

	public DataFrame getTargets() {
		return targets;
	}

	/**
	 * @since 3.0
	 */
	public DeleteSegment setTargets(DataFrame targets) {
		this.targets = targets;
		return this;
	}

	/**
	 * @since 1.6
	 */
	public Set<Object> getSourceKeys() {
		return sourceKeys;
	}

	/**
	 * @since 1.6
	 */
	public DeleteSegment setSourceKeys(Set<Object> sourceKeys) {
		this.sourceKeys = sourceKeys;
		return this;
	}

	public DataFrame getMappedTargets() {
		return mappedTargets;
	}

	public DeleteSegment setMappedTargets(DataFrame mappedTargets) {
		this.mappedTargets = mappedTargets;
		return this;
	}

	public DataFrame getMissingTargets() {
		return missingTargets;
	}

	public DeleteSegment setMissingTargets(DataFrame missingTargets) {
		this.missingTargets = missingTargets;
		return this;
	}
}
