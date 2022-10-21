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
        logger.debug("[{}/{}] batch started", exec.getTaskName(), exec.getId());
    }

    @Override
    public void deleteBatchFinished(Execution exec, int objectsProcessed, int objectsDeleted) {
        logger.debug("[{}/{}] batch finished, processed {}, deleted {} object(s)",
                exec.getTaskName(),
                exec.getId(),
                objectsProcessed,
                objectsDeleted);
    }

    @Override
    public void createBatchFinished(Execution exec, int rowsProcessed, int objectsInserted) {
        logger.debug("[{}/{}] batch finished, processed {} row(s), created {} object(s)",
                exec.getTaskName(),
                exec.getId(),
                rowsProcessed,
                objectsInserted);
    }

    @Override
    public void createOrUpdateBatchFinished(Execution exec, int rowsProcessed, int objectsInserted, int objectsUpdated) {
        logger.debug("[{}/{}] batch finished, processed {} row(s), created {}, updated {} object(s)",
                exec.getTaskName(),
                exec.getId(),
                rowsProcessed,
                objectsInserted,
                objectsUpdated);
    }

    @Override
    public void sourceKeysBatchFinished(Execution exec, int rowsProcessed, int keysExtracted) {
        logger.debug("[{}/{}] batch finished, processed {} row(s), extracted {} keys(s)",
                exec.getTaskName(),
                exec.getId(),
                rowsProcessed,
                keysExtracted);
    }
}
