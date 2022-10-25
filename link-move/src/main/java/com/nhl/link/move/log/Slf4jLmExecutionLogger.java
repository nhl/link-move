package com.nhl.link.move.log;

import com.nhl.dflib.Series;
import com.nhl.link.move.Execution;
import com.nhl.link.move.ExecutionStats;
import com.nhl.link.move.extractor.model.ExtractorName;
import com.nhl.link.move.runtime.task.sourcekeys.SourceKeysTask;
import org.apache.cayenne.Persistent;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.List;
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

        ExtractorName extractorName = exec.getExtractorName();
        if (extractorName != null) {
            if (extractorName.getName() != null && !ExtractorName.DEFAULT_NAME.equals(extractorName.getName())) {
                logger.info("{} extractor:{}:{}", label, extractorName.getLocation(), extractorName.getName());
            } else {
                logger.info("{} extractor:{}", label, extractorName.getLocation());
            }
        }
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
    public void deleteSegmentFinished(int objectsProcessed, Series<? extends Persistent> objectsDeleted) {

        if (objectsDeleted.size() > 0 && logger.isTraceEnabled()) {

            logger.trace("{} segment:{} deleted_ids:{}",
                    label,
                    exec.getStats().getSegments(),
                    loggableIds(objectsDeleted));
        }

        logger.debug("{} segment:{} done in:{} out_deleted:{}",
                label,
                exec.getStats().getSegments(),
                objectsProcessed,
                objectsDeleted.size());
    }

    @Override
    public void createSegmentFinished(int rowsProcessed, Series<? extends Persistent> objectsInserted) {
        if (objectsInserted.size() > 0 && logger.isTraceEnabled()) {

            logger.trace("{} segment:{} created_ids:{}",
                    label,
                    exec.getStats().getSegments(),
                    loggableIds(objectsInserted));
        }

        logger.debug("{} segment:{} done in:{} out_created:{}",
                label,
                exec.getStats().getSegments(),
                rowsProcessed,
                objectsInserted.size());
    }

    @Override
    public void createOrUpdateSegmentFinished(
            int rowsProcessed,
            Series<? extends Persistent> objectsInserted,
            Series<? extends Persistent> objectsUpdated) {

        if (logger.isTraceEnabled()) {
            if (objectsInserted.size() > 0) {

                logger.trace("{} segment:{} created_ids:{}",
                        label,
                        exec.getStats().getSegments(),
                        loggableIds(objectsInserted));
            }

            if (objectsUpdated.size() > 0) {

                logger.trace("{} segment:{} updated_ids:{}",
                        label,
                        exec.getStats().getSegments(),
                        loggableIds(objectsUpdated));
            }
        }

        logger.debug("{} segment:{} done in:{} out_created:{} out_updated:{}",
                label,
                exec.getStats().getSegments(),
                rowsProcessed,
                objectsInserted.size(),
                objectsUpdated.size());
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

    private List<?> loggableIds(Series<? extends Persistent> objects) {

        if (objects.size() == 0) {
            return Collections.emptyList();
        }

        boolean singleColumnId = objects.get(0).getObjectId().getIdSnapshot().size() == 1;
        return singleColumnId
                ? objects.map(p -> p.getObjectId().getIdSnapshot().values().iterator().next()).toList()
                : objects.map(p -> p.getObjectId().getIdSnapshot()).toList();
    }
}
