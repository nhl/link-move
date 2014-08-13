package com.nhl.link.etl;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Represents a single execution of an {@link EtlTask}. Tracks task parameters
 * and execution statistics.
 */
public class Execution implements AutoCloseable {

	protected SyncToken syncToken;
	protected long extracted;
	protected long created;
	protected long updated;
	protected long started;
	protected long finished;

	public Execution(SyncToken token) {
		this.started = System.currentTimeMillis();
		this.syncToken = token;
	}

	@Override
	public void close() {
		this.finished = System.currentTimeMillis();
	}

	@Override
	public String toString() {
		return createReport().toString();
	}

	/**
	 * Creates task execution report as a map of labels vs. values.
	 */
	public Map<String, Object> createReport() {
		// let's keep order of insertion consistent so that the report is easily
		// printable
		Map<String, Object> report = new LinkedHashMap<>();

		report.put("Task", syncToken.getName());
		report.put("Token", syncToken.getValue());

		DateFormat format = new SimpleDateFormat("YYYY-mm-dd HH:MM:SS");

		if (finished > 0) {
			report.put("Status", "finished");
			report.put("Duration, ms.", finished - started);
		} else {
			report.put("Status", "in progress");
			report.put("Started on ", format.format(new Date(started)));
		}

		report.put("Extracted", extracted);
		report.put("Created", created);
		report.put("Updated", updated);

		return report;
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

	public SyncToken getSyncToken() {
		return syncToken;
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

	public long getStarted() {
		return started;
	}

	public long getFinished() {
		return finished;
	}

}
