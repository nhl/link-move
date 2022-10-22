package com.nhl.link.move.runtime.task.delete;

import com.nhl.dflib.DataFrame;
import org.apache.cayenne.ObjectContext;

import java.util.Set;

public class DeleteSegment {

	public static final String TARGET_COLUMN = "$lm_target";
	public static final String KEY_COLUMN = "$lm_key";

	private ObjectContext context;
	private DataFrame targets;

	private Set<Object> sourceKeys;
	private DataFrame mappedTargets;
	private DataFrame missingTargets;

	public DeleteSegment(ObjectContext context, DataFrame targets) {
		this.targets = targets;
		this.context = context;
	}

	public ObjectContext getContext() {
		return context;
	}

	public DataFrame getTargets() {
		return targets;
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
	public void setSourceKeys(Set<Object> sourceKeys) {
		this.sourceKeys = sourceKeys;
	}

	public DataFrame getMappedTargets() {
		return mappedTargets;
	}

	public void setMappedTargets(DataFrame mappedTargets) {
		this.mappedTargets = mappedTargets;
	}

	public DataFrame getMissingTargets() {
		return missingTargets;
	}

	public void setMissingTargets(DataFrame missingTargets) {
		this.missingTargets = missingTargets;
	}
}
