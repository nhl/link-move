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
        logger.info("[{}/{}] exec", exec.getId(), exec.getTaskName());
    }

    @Override
    public void createExecFinished(Execution exec) {
        ExecutionStats stats = exec.getStats();

        logger.info("[{}/{}] exec done time:{} batches:{} in:{} out_created:{}",
                exec.getId(),
                exec.getTaskName(),
                stats.getDuration(),
                stats.getSegments(),
                stats.getExtracted(),
                stats.getCreated());
    }

    @Override
    public void createOrUpdateExecFinished(Execution exec) {
        ExecutionStats stats = exec.getStats();

        logger.info("[{}/{}] exec done time:{} segments:{} in:{} out_created:{} out_updated:{}",
                exec.getId(),
                exec.getTaskName(),
                stats.getDuration(),
                stats.getSegments(),
                stats.getExtracted(),
                stats.getCreated(),
                stats.getUpdated());
    }

    @Override
    public void deleteExecFinished(Execution exec) {
        ExecutionStats stats = exec.getStats();

        logger.info("[{}/{}] exec done time:{} segments:{} in:{} out_deleted:{}",
                exec.getId(),
                exec.getTaskName(),
                stats.getDuration(),
                stats.getSegments(),
                stats.getExtracted(),
                stats.getDeleted());
    }

    @Override
    public void sourceKeysExecFinished(Execution exec) {

        if (logger.isInfoEnabled()) {

            Set<?> keys = (Set<?>) exec.getAttribute(SourceKeysTask.RESULT_KEY);
            Set<?> keysReported = keys != null ? keys : Collections.emptySet();

            ExecutionStats stats = exec.getStats();

            logger.info("[{}/{}] exec done time:{} segments:{} in:{} out_keys:{}",
                    exec.getId(),
                    exec.getTaskName(),
                    stats.getDuration(),
                    stats.getSegments(),
                    stats.getExtracted(),
                    keysReported.size());
        }
    }

    @Override
    public void segmentStarted(Execution exec) {
        logger.debug("[{}/{}] segment:{}", exec.getId(), exec.getTaskName(), exec.getStats().getSegments());
    }

    @Override
    public void deleteSegmentFinished(Execution exec, int objectsProcessed, int objectsDeleted) {
        logger.debug("[{}/{}] segment:{} done in:{} out_deleted:{}",
                exec.getId(),
                exec.getTaskName(),
                exec.getStats().getSegments(),
                objectsProcessed,
                objectsDeleted);
    }

    @Override
    public void createSegmentFinished(Execution exec, int rowsProcessed, int objectsInserted) {
        logger.debug("[{}/{}] segment:{} done in:{} out_created:{}",
                exec.getId(),
                exec.getTaskName(),
                exec.getStats().getSegments(),
                rowsProcessed,
                objectsInserted);
    }

    @Override
    public void createOrUpdateSegmentFinished(Execution exec, int rowsProcessed, int objectsInserted, int objectsUpdated) {
        logger.debug("[{}/{}] segment:{} done in:{} out_created:{} out_updated:{}",
                exec.getId(),
                exec.getTaskName(),
                exec.getStats().getSegments(),
                rowsProcessed,
                objectsInserted,
                objectsUpdated);
    }

    @Override
    public void sourceKeysSegmentFinished(Execution exec, int rowsProcessed, Set<?> keysExtracted) {
        if (!keysExtracted.isEmpty() && logger.isTraceEnabled()) {
            logger.trace("[{}/{}] segment:{} out_keys:{}",
                    exec.getId(),
                    exec.getTaskName(),
                    exec.getStats().getSegments(),
                    keysExtracted);
        }

        logger.debug("[{}/{}] segment:{} done in:{} out_keys:{}",
                exec.getId(),
                exec.getTaskName(),
                exec.getStats().getSegments(),
                rowsProcessed,
                keysExtracted.size());
    }
}
