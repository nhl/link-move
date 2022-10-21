package com.nhl.link.move;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * @since 1.3
 */
public class ExecutionStats {

    protected long extracted;
    protected long created;
    protected long updated;
    protected long deleted;

    protected LocalDateTime startedOn;
    protected LocalDateTime stoppedOn;

    public ExecutionStats executionStarted() {
        this.startedOn = LocalDateTime.now();
        this.stoppedOn = null;
        return this;
    }

    public void executionStopped() {
        this.stoppedOn = LocalDateTime.now();
    }

    public boolean isStopped() {
        return stoppedOn != null;
    }

    /**
     * @since 3.0
     */
    public Duration getDuration() {
        return stoppedOn != null ? Duration.between(startedOn, stoppedOn) : null;
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

    /**
     * @since 3.0
     */
    public LocalDateTime getStartedOn() {
        return startedOn;
    }

    /**
     * @since 3.0
     */
    public LocalDateTime getStoppedOn() {
        return stoppedOn;
    }


    /**
     * @deprecated since 3.0 in favor of {@link #getStartedOn()}
     */
    @Deprecated(since = "3.0")
    public long getStarted() {
        return startedOn != null ? startedOn.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli() : 0;
    }

    /**
     * @deprecated since 3.0 in favor of {@link #getStoppedOn()}
     */
    @Deprecated(since = "3.0")
    public long getFinished() {
        return stoppedOn != null ? stoppedOn.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli() : 0;
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

}
