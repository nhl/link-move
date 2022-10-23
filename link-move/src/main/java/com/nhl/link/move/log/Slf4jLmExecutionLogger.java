package com.nhl.link.move.log;

import com.nhl.link.move.Execution;
import com.nhl.link.move.ExecutionStats;
import com.nhl.link.move.runtime.task.sourcekeys.SourceKeysTask;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.Set;

/**
 * @since 3.0
 */
public class Slf4jLmExecutionLogger implements LmExecutionLogger {

    private final Logger logger;
    private final Execution exec;
    private final String label;

    public Slf4jLmExecutionLogger(Logger logger, Execution exec) {
        this.logger = logger;
        this.exec = exec;

        // if no logging is expected to occur, just use a placeholder for the log label
        this.label = logger.isInfoEnabled() ? execLabel() : "exec";
    }

    @Override
    public void execStarted() {
        logger.info("{} start", label);
    }

    @Override
    public void createExecFinished() {
        ExecutionStats stats = exec.getStats();

        logger.info("{} done time:{} batches:{} in:{} out_created:{}",
                label,
                stats.getDuration(),
                stats.getSegments(),
                stats.getExtracted(),
                stats.getCreated());
    }

    @Override
    public void createOrUpdateExecFinished() {
        ExecutionStats stats = exec.getStats();

        logger.info("{} done time:{} segments:{} in:{} out_created:{} out_updated:{}",
                label,
                stats.getDuration(),
                stats.getSegments(),
                stats.getExtracted(),
                stats.getCreated(),
                stats.getUpdated());
    }

    @Override
    public void deleteExecFinished() {

        ExecutionStats stats = exec.getStats();

        logger.info("{} done time:{} segments:{} in:{} out_deleted:{}",
                label,
                stats.getDuration(),
                stats.getSegments(),
                stats.getExtracted(),
                stats.getDeleted());
    }

    @Override
    public void sourceKeysExecFinished() {

        Set<?> keys = (Set<?>) exec.getAttribute(SourceKeysTask.RESULT_KEY);
        Set<?> keysReported = keys != null ? keys : Collections.emptySet();

        ExecutionStats stats = exec.getStats();

        logger.info("{} done time:{} segments:{} in:{} out_keys:{}",
                label,
                stats.getDuration(),
                stats.getSegments(),
                stats.getExtracted(),
                keysReported.size());
    }

    @Override
    public void segmentStarted() {
        logger.debug("{} segment:{} start", label, exec.getStats().getSegments());
    }

    @Override
    public void deleteSegmentFinished(int objectsProcessed, int objectsDeleted) {
        logger.debug("{} segment:{} done in:{} out_deleted:{}",
                label,
                exec.getStats().getSegments(),
                objectsProcessed,
                objectsDeleted);
    }

    @Override
    public void createSegmentFinished(int rowsProcessed, int objectsInserted) {
        logger.debug("{} segment:{} done in:{} out_created:{}",
                label,
                exec.getStats().getSegments(),
                rowsProcessed,
                objectsInserted);
    }

    @Override
    public void createOrUpdateSegmentFinished(int rowsProcessed, int objectsInserted, int objectsUpdated) {
        logger.debug("{} segment:{} done in:{} out_created:{} out_updated:{}",
                label,
                exec.getStats().getSegments(),
                rowsProcessed,
                objectsInserted,
                objectsUpdated);
    }

    @Override
    public void sourceKeysSegmentFinished(int rowsProcessed, Set<?> keysExtracted) {

        if (!keysExtracted.isEmpty() && logger.isTraceEnabled()) {
            logger.trace("{} segment:{} out_keys:{}",
                    label,
                    exec.getStats().getSegments(),
                    keysExtracted);
        }

        logger.debug("{} segment:{} done in:{} out_keys:{}",
                label,
                exec.getStats().getSegments(),
                rowsProcessed,
                keysExtracted.size());
    }

    private String execLabel() {
        return exec.getParentExecution() != null
                ? "[" + exec.getId() + "/" + exec.getParentExecution().getTaskName() + "/" + exec.getTaskName() + "]"
                : "[" + exec.getId() + "/" + exec.getTaskName() + "]";
    }
}
