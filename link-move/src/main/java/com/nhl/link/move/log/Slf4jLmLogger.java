package com.nhl.link.move.log;

import com.nhl.link.move.Execution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @since 3.0
 */
public class Slf4jLmLogger implements LmLogger {

    private final Logger logger;

    public Slf4jLmLogger() {
        this.logger = LoggerFactory.getLogger(LmLogger.class);
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
    public void sourceKeysBatchFinished(Execution exec, int rowsProcessed, int keysExtracted) {
        logger.debug("[{}/{}] batch:{} done in:{}, out_keys:{}",
                exec.getId(),
                exec.getTaskName(),
                exec.getStats().getBatches(),
                rowsProcessed,
                keysExtracted);
    }
}
