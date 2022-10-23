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
                stats.getBatches(),
                stats.getExtracted(),
                stats.getCreated());
    }

    @Override
    public void createOrUpdateExecFinished(Execution exec) {
        ExecutionStats stats = exec.getStats();

        logger.info("[{}/{}] exec done time:{} batches:{} in:{} out_created:{} out_updated:{}",
                exec.getId(),
                exec.getTaskName(),
                stats.getDuration(),
                stats.getBatches(),
                stats.getExtracted(),
                stats.getCreated(),
                stats.getUpdated());
    }

    @Override
    public void deleteExecFinished(Execution exec) {
        ExecutionStats stats = exec.getStats();

        logger.info("[{}/{}] exec done time:{} batches:{} in:{} out_deleted:{}",
                exec.getId(),
                exec.getTaskName(),
                stats.getDuration(),
                stats.getBatches(),
                stats.getExtracted(),
                stats.getDeleted());
    }

    @Override
    public void sourceKeysExecFinished(Execution exec) {

        if (logger.isInfoEnabled()) {

            Set<?> keys = (Set<?>) exec.getAttribute(SourceKeysTask.RESULT_KEY);
            Set<?> keysReported = keys != null ? keys : Collections.emptySet();
            
            ExecutionStats stats = exec.getStats();

            logger.info("[{}/{}] exec done time:{} batches:{} in:{} out_keys:{}",
                    exec.getId(),
                    exec.getTaskName(),
                    stats.getDuration(),
                    stats.getBatches(),
                    stats.getExtracted(),
                    keysReported.size());
        }
    }

    @Override
    public void batchStarted(Execution exec) {
        logger.debug("[{}/{}] batch:{}", exec.getId(), exec.getTaskName(), exec.getStats().getBatches());
    }

    @Override
    public void deleteBatchFinished(Execution exec, int objectsProcessed, int objectsDeleted) {
        logger.debug("[{}/{}] batch:{} done in:{} out_deleted:{}",
                exec.getId(),
                exec.getTaskName(),
                exec.getStats().getBatches(),
                objectsProcessed,
                objectsDeleted);
    }

    @Override
    public void createBatchFinished(Execution exec, int rowsProcessed, int objectsInserted) {
        logger.debug("[{}/{}] batch:{} done in:{} out_created:{}",
                exec.getId(),
                exec.getTaskName(),
                exec.getStats().getBatches(),
                rowsProcessed,
                objectsInserted);
    }

    @Override
    public void createOrUpdateBatchFinished(Execution exec, int rowsProcessed, int objectsInserted, int objectsUpdated) {
        logger.debug("[{}/{}] batch:{} done in:{} out_created:{} out_updated:{}",
                exec.getId(),
                exec.getTaskName(),
                exec.getStats().getBatches(),
                rowsProcessed,
                objectsInserted,
                objectsUpdated);
    }

    @Override
    public void sourceKeysBatchFinished(Execution exec, int rowsProcessed, Set<?> keysExtracted) {
        if (!keysExtracted.isEmpty() && logger.isTraceEnabled()) {
            logger.trace("[{}/{}] batch:{} out_keys:{}",
                    exec.getId(),
                    exec.getTaskName(),
                    exec.getStats().getBatches(),
                    keysExtracted);
        }

        logger.debug("[{}/{}] batch:{} done in:{} out_keys:{}",
                exec.getId(),
                exec.getTaskName(),
                exec.getStats().getBatches(),
                rowsProcessed,
                keysExtracted.size());
    }
}
