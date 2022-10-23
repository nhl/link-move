package com.nhl.link.move.log;

import com.nhl.link.move.Execution;
import com.nhl.link.move.ExecutionStats;
import com.nhl.link.move.runtime.task.sourcekeys.SourceKeysTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Set;

/**
 * @since 3.0
 */
public class Slf4jLmLogger implements LmLogger {

    private final Logger logger;

    public Slf4jLmLogger() {
        this.logger = LoggerFactory.getLogger(LmLogger.class);
    }

    @Override
    public void execStarted(Execution exec) {
        if (logger.isInfoEnabled()) {
            logger.info("[{}] start", execLabel(exec));
        }
    }

    @Override
    public void createExecFinished(Execution exec) {

        if (logger.isInfoEnabled()) {
            ExecutionStats stats = exec.getStats();

            logger.info("[{}] done time:{} batches:{} in:{} out_created:{}",
                    execLabel(exec),
                    stats.getDuration(),
                    stats.getSegments(),
                    stats.getExtracted(),
                    stats.getCreated());
        }
    }

    @Override
    public void createOrUpdateExecFinished(Execution exec) {
        if (logger.isInfoEnabled()) {
            ExecutionStats stats = exec.getStats();

            logger.info("[{}] done time:{} segments:{} in:{} out_created:{} out_updated:{}",
                    execLabel(exec),
                    stats.getDuration(),
                    stats.getSegments(),
                    stats.getExtracted(),
                    stats.getCreated(),
                    stats.getUpdated());
        }
    }

    @Override
    public void deleteExecFinished(Execution exec) {
        if (logger.isInfoEnabled()) {
            ExecutionStats stats = exec.getStats();

            logger.info("[{}] done time:{} segments:{} in:{} out_deleted:{}",
                    execLabel(exec),
                    stats.getDuration(),
                    stats.getSegments(),
                    stats.getExtracted(),
                    stats.getDeleted());
        }
    }

    @Override
    public void sourceKeysExecFinished(Execution exec) {
        if (logger.isInfoEnabled()) {

            Set<?> keys = (Set<?>) exec.getAttribute(SourceKeysTask.RESULT_KEY);
            Set<?> keysReported = keys != null ? keys : Collections.emptySet();

            ExecutionStats stats = exec.getStats();

            logger.info("[{}] done time:{} segments:{} in:{} out_keys:{}",
                    execLabel(exec),
                    stats.getDuration(),
                    stats.getSegments(),
                    stats.getExtracted(),
                    keysReported.size());
        }
    }

    @Override
    public void segmentStarted(Execution exec) {
        if (logger.isDebugEnabled()) {
            logger.debug("[{}] segment:{} start", execLabel(exec), exec.getStats().getSegments());
        }
    }

    @Override
    public void deleteSegmentFinished(Execution exec, int objectsProcessed, int objectsDeleted) {
        if (logger.isDebugEnabled()) {
            logger.debug("[{}] segment:{} done in:{} out_deleted:{}",
                    execLabel(exec),
                    exec.getStats().getSegments(),
                    objectsProcessed,
                    objectsDeleted);
        }
    }

    @Override
    public void createSegmentFinished(Execution exec, int rowsProcessed, int objectsInserted) {
        if (logger.isDebugEnabled()) {
            logger.debug("[{}] segment:{} done in:{} out_created:{}",
                    execLabel(exec),
                    exec.getStats().getSegments(),
                    rowsProcessed,
                    objectsInserted);
        }
    }

    @Override
    public void createOrUpdateSegmentFinished(Execution exec, int rowsProcessed, int objectsInserted, int objectsUpdated) {
        if (logger.isDebugEnabled()) {
            logger.debug("[{}] segment:{} done in:{} out_created:{} out_updated:{}",
                    execLabel(exec),
                    exec.getStats().getSegments(),
                    rowsProcessed,
                    objectsInserted,
                    objectsUpdated);
        }
    }

    @Override
    public void sourceKeysSegmentFinished(Execution exec, int rowsProcessed, Set<?> keysExtracted) {
        if (logger.isDebugEnabled()) {

            String label = execLabel(exec);

            if (!keysExtracted.isEmpty() && logger.isTraceEnabled()) {
                logger.trace("[{}] segment:{} out_keys:{}",
                        label,
                        exec.getStats().getSegments(),
                        keysExtracted);
            }

            logger.debug("[{}] segment:{} done in:{} out_keys:{}",
                    label,
                    exec.getStats().getSegments(),
                    rowsProcessed,
                    keysExtracted.size());
        }
    }

    private String execLabel(Execution exec) {
        return exec.getParentExecution() != null
                ? exec.getId() + "/" + exec.getParentExecution().getTaskName() + "/" + exec.getTaskName()
                : exec.getId() + "/" + exec.getTaskName();
    }
}
