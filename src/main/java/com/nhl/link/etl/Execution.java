package com.nhl.link.etl;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
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
	protected Map<String, Object> attributes;
	protected ExecutionStats stats;

	public Execution(String name, Map<String, ?> params) {
		this.name = name;
		this.parameters = params;

		// a "parallel" execution should have this turned into a ConcurrentMap
		this.attributes = new HashMap<>();

		this.stats = new ExecutionStats();

		stats.executionStarted();
	}

	@Override
	public void close() {
		stats.executionStopped();
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

		if (stats.isStopped()) {
			report.put("Status", "finished");
			report.put("Duration", stats.getDuration());
		} else {
			report.put("Status", "in progress");
			report.put("Started on ", format.format(new Date(stats.getStarted())));
		}

		report.put("Extracted", stats.getExtracted());
		report.put("Created", stats.getCreated());
		report.put("Updated", stats.getUpdated());

		return report;
	}

	/**
	 * @since 1.3
	 */
	public Object getAttribute(String key) {
		return attributes.get(key);
	}

	/**
	 * @since 1.3
	 */
	public void setAttribute(String key, Object value) {
		attributes.put(key, value);
	}

	/**
	 * @since 1.3
	 */
	public Map<String, ?> getParameters() {
		return parameters;
	}

	public ExecutionStats getStats() {
		return stats;
	}

}
