package com.nhl.link.etl;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Represents a single execution of an {@link EtlTask}. Tracks task parameters
 * and execution statistics.
 */
public class Execution implements AutoCloseable {

	protected String name;
	protected Map<String, ?> parameters;
	protected long extracted;
	protected long created;
	protected long updated;
	protected long started;
	protected long finished;

	public Execution(String name, Map<String, ?> params) {
		this.started = System.currentTimeMillis();
		this.name = name;
		this.parameters = params;
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

		if (name != null) {
			report.put("Task", name);
		}

		for (Entry<String, ?> p : parameters.entrySet()) {
			report.put("Parameter[" + p.getKey() + "]", p.getValue());
		}

		DateFormat format = new SimpleDateFormat("YYYY-mm-dd HH:MM:SS");

		if (finished > 0) {
			report.put("Status", "finished");
			report.put("Duration", finished - started);
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

	/**
	 * @since 1.3
	 */
	public Map<String, ?> getParameters() {
		return parameters;
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
