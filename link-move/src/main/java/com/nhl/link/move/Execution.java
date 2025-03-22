package com.nhl.link.move;

import com.nhl.link.move.extractor.model.ExtractorName;
import com.nhl.link.move.log.LmExecutionLogger;
import com.nhl.link.move.log.LmLogger;
import com.nhl.link.move.log.LoggableJson;

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
    protected final LmExecutionLogger logger;
    protected final ExecutionStats stats;
    protected final Execution parentExecution;

    /**
     * @since 3.0.0
     */
    public Execution(
            long id,
            String taskName,
            ExtractorName extractorName,
            Map<String, ?> params,
            LmLogger logger,
            Execution parentExecution) {

        this.id = id;
        this.taskName = taskName;
        this.extractorName = extractorName;
        this.parameters = params;
        this.attributes = new ConcurrentHashMap<>();
        this.parentExecution = parentExecution;
        this.stats = new ExecutionStats().executionStarted();

        // must initialize every other variable before initializing the logger that depends on the exec state
        this.logger = logger.executionLogger(this);
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
            LoggableJson.append(paramsOut, p.getKey(), p.getValue());
        }
        paramsOut.append("}");

        StringBuilder statsOut = new StringBuilder("{");
        LoggableJson.append(statsOut, "batches", stats.getSegments());
        LoggableJson.append(statsOut, "created", stats.getCreated());
        LoggableJson.append(statsOut, "deleted", stats.getDeleted());
        LoggableJson.append(statsOut, "duration", stats.getDuration());
        LoggableJson.append(statsOut, "extracted", stats.getExtracted());
        LoggableJson.append(statsOut, "startedOn", stats.getStartedOn());
        LoggableJson.append(statsOut, "status", stats.isStopped() ? "finished" : "in progress");
        LoggableJson.append(statsOut, "updated", stats.getUpdated());

        statsOut.append("}");

        StringBuilder out = new StringBuilder("{");
        LoggableJson.append(out, "id", id);
        LoggableJson.append(out, "extractor", extractorName);
        LoggableJson.append(out, "parameters", paramsOut.toString(), false);
        LoggableJson.append(out, "stats", statsOut.toString(), false);
        LoggableJson.append(out, "task", taskName);

        return out.append("}").toString();
    }



    /**
     * Returns execution id. It is an incrementing number and is unique within a JVM. It is used primarily for
     * logging, so there's no goal to keep it globally unique.
     *
     * @since 3.0.0
     */
    public long getId() {
        return id;
    }

    /**
     * @since 3.0.0
     */
    public String getTaskName() {
        return taskName;
    }

    /**
     * @since 3.0.0
     */
    public ExtractorName getExtractorName() {
        return extractorName;
    }

    /**
     * @since 3.0.0
     */
    public Execution getParentExecution() {
        return parentExecution;
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
     * @since 3.0.0
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
     * @since 3.0.0
     */
    public LmExecutionLogger getLogger() {
        return logger;
    }

    public ExecutionStats getStats() {
        return stats;
    }
}
