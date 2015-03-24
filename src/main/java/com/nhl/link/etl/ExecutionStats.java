package com.nhl.link.etl;

/**
 * @since 1.3
 */
public class ExecutionStats {

	protected long extracted;
	protected long created;
	protected long updated;
	protected long deleted;
	protected long started;
	protected long finished;

	public void executionStarted() {
		this.started = System.currentTimeMillis();
		finished = 0;
	}

	public void executionStopped() {
		this.finished = System.currentTimeMillis();
	}
	
	public boolean isStopped() {
		return finished > 0 && finished > started;
	}
	
	public long getDuration() {
		return finished - started;
	}

	public void incrementExtracted(long count) {
		this.extracted += count;
	}

	public void incrementCreated(long count) {
		this.created += count;
	}

	public void incrementUpdated(long count) {
		this.updated += count;
	}
	
	public void incrementDeleted(long count) {
		this.deleted += count;
	}

	public long getExtracted() {
		return extracted;
	}

	public long getCreated() {
		return created;
	}

	public long getUpdated() {
		return updated;
	}
	
	public long getDeleted() {
		return deleted;
	}

	public long getStarted() {
		return started;
	}

	public long getFinished() {
		return finished;
	}

}
