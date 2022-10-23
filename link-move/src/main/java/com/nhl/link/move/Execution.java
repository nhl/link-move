package com.nhl.link.move;

import com.nhl.link.move.extractor.model.ExtractorName;
import com.nhl.link.move.log.LmLogger;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * A single execution of an {@link LmTask}. Tracks task parameters and execution statistics.
 */
public class Execution {

    protected final long id;
    protected final String taskName;
    protected final ExtractorName extractorName;
    protected final Map<String, ?> parameters;
    protected final Map<String, Object> attributes;
    protected final LmLogger logger;
    protected final ExecutionStats stats;

    /**
     * @since 3.0
     */
    public Execution(long id, String taskName, ExtractorName extractorName, Map<String, ?> params, LmLogger logger) {
        this.id = id;
        this.taskName = taskName;
        this.extractorName = extractorName;
        this.parameters = params;
        this.attributes = new ConcurrentHashMap<>();
        this.logger = logger;
        this.stats = new ExecutionStats().executionStarted();
    }

    /**
     * @deprecated in favor of {@link #stop()}
     */
    @Deprecated(since = "3.0")
    public void close() {
        stop();
    }

    /**
     * Record the timestamp for stats reporting.
     */
    public void stop() {
        stats.executionStopped();
    }

    @Override
    public String toString() {

        // generate JSON-ish output

        StringBuilder paramsOut = new StringBuilder("{");
        for (Entry<String, ?> p : parameters.entrySet()) {
            append(paramsOut, p.getKey(), p.getValue());
        }
        paramsOut.append("}");

        StringBuilder statsOut = new StringBuilder("{");
        append(statsOut, "batches", stats.getBatches());
        append(statsOut, "created", stats.getCreated());
        append(statsOut, "deleted", stats.getDeleted());
        append(statsOut, "duration", stats.getDuration());
        append(statsOut, "extracted", stats.getExtracted());
        append(statsOut, "startedOn", stats.getStartedOn());
        append(statsOut, "status", stats.isStopped() ? "finished" : "in progress");
        append(statsOut, "updated", stats.getUpdated());

        statsOut.append("}");

        StringBuilder out = new StringBuilder("{");
        append(out, "id", id);
        append(out, "extractor", extractorName);
        append(out, "parameters", paramsOut.toString(), false);
        append(out, "stats", statsOut.toString(), false);
        append(out, "task", taskName);

        return out.append("}").toString();
    }

    private void append(StringBuilder out, String key, Object val) {
        append(out, key, val, !(val instanceof Number));
    }

    private void append(StringBuilder out, String key, Object val, boolean quote) {
        if (val == null) {
            return;
        }

        if (out.length() > 1) {
            out.append(',');
        }

        out.append("\"").append(key).append("\":");

        if (quote) {
            out.append("\"").append(val).append("\"");

        } else {
            out.append(val);
        }
    }

    /**
     * Returns execution id. It is an incrementing number and is unique within a JVM. It is used primarily for
     * logging, so there's no goal to keep it globally unique.
     *
     * @since 3.0
     */
    public long getId() {
        return id;
    }

    /**
     * @since 2.8
     * @deprecated since 3.0 {@link #getExtractorName()} and {@link #getTaskName()} are used instead to identify the
     * execution.
     */
    @Deprecated(since = "3.0")
    public String getName() {
        return taskName + ":" + extractorName;
    }

    /**
     * @since 3.0
     */
    public String getTaskName() {
        return taskName;
    }

    /**
     * @since 3.0
     */
    public ExtractorName getExtractorName() {
        return extractorName;
    }

    /**
     * Creates task execution report as a map of labels vs. values.
     *
     * @deprecated since 3.0. Execution reports are used primarily for logging, which is now handled by
     * {@link com.nhl.link.move.log.LmLogger}, so this API is no longer useful. The closest approximation
     * of the "report" is a {@link #toString()} method.
     */
    @Deprecated(since = "3.0")
    public Map<String, Object> createReport() {

        // keep order of insertion consistent so that the report is easily printable
        Map<String, Object> report = new LinkedHashMap<>();

        report.put("Task", getName());

        for (Entry<String, ?> p : parameters.entrySet()) {
            report.put("Parameter[" + p.getKey() + "]", p.getValue());
        }

        if (stats.isStopped()) {
            report.put("Status", "finished");
            report.put("Started on", stats.getStartedOn());
            report.put("Duration", stats.getDuration());
        } else {
            report.put("Status", "in progress");
            report.put("Started on", stats.getStartedOn());
        }

        report.put("Extracted", stats.getExtracted());
        report.put("Created", stats.getCreated());
        report.put("Updated", stats.getUpdated());
        report.put("Deleted", stats.getDeleted());

        return report;
    }

    /**
     * @since 1.3
     */
    public Object getAttribute(String key) {
        return attributes.get(key);
    }

    /**
     * Sets a custom execution-scoped attribute. Passing a null value would remove the attribute. Attribute mechanism
     * is used as way to communicate state between processed batches (segments), and between listeners.
     *
     * @since 1.3
     */
    public void setAttribute(String key, Object value) {
        if (value == null) {
            attributes.remove(key);
        } else {
            attributes.put(key, value);
        }
    }

    /**
     * Returns an execution-scoped attribute value. If no such attribute is previously stored, its value is created
     * using the provided function, and cached for the future use for the given key. Attribute mechanism
     * is used as way to communicate state between processed batches (segments), and between listeners.
     *
     * @since 3.0
     */
    public <T> T computeAttributeIfAbsent(String key, Function<String, T> valueProducer) {
        return (T) attributes.computeIfAbsent(key, valueProducer);
    }

    /**
     * Returns task execution parameters.
     *
     * @since 1.3
     */
    public Map<String, ?> getParameters() {
        return parameters;
    }

    /**
     * @since 3.0
     */
    public LmLogger getLogger() {
        return logger;
    }

    public ExecutionStats getStats() {
        return stats;
    }
}
