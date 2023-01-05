package com.nhl.link.move.runtime.task.delete;

import com.nhl.dflib.DataFrame;
import com.nhl.dflib.Index;
import com.nhl.dflib.Series;
import com.nhl.link.move.Execution;
import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.batch.BatchProcessor;
import com.nhl.link.move.log.LmLogger;
import com.nhl.link.move.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.move.runtime.task.BaseTask;
import com.nhl.link.move.runtime.task.sourcekeys.SourceKeysTask;
import com.nhl.link.move.runtime.token.ITokenManager;
import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.Persistent;
import org.apache.cayenne.ResultIterator;
import org.apache.cayenne.exp.Expression;
import org.apache.cayenne.query.ObjectSelect;

import java.util.Collections;
import java.util.Set;

/**
 * A task that allows to delete target objects not present in the source.
 *
 * @since 1.3
 */
public class DeleteTask extends BaseTask {

    private final Class<?> type;
    private final Expression targetFilter;
    private final LmTask sourceKeysSubtask;
    private final DeleteSegmentProcessor processor;
    private final ITargetCayenneService targetCayenneService;

    public DeleteTask(
            int batchSize,
            Class<?> type,
            Expression targetFilter,
            ITargetCayenneService targetCayenneService,
            ITokenManager tokenManager,
            LmTask sourceKeysSubtask,
            DeleteSegmentProcessor processor,
            LmLogger logger) {

        // extractor is not used by the "delete" task, only by its key extraction subtask,
        // so set it to null

        super(null, batchSize, tokenManager, logger);

        this.type = type;
        this.targetFilter = targetFilter;
        this.targetCayenneService = targetCayenneService;
        this.sourceKeysSubtask = sourceKeysSubtask;
        this.processor = processor;
    }

    @Override
    protected String createLabel() {
        return "delete";
    }

    @Override
    protected void doRun(Execution exec) {


        try (ResultIterator data = createTargetSelect(exec)) {

            // do not preload the keys if there are no target objects
            Set<Object> keys = data.hasNextRow() ? loadKeys(exec) : Collections.emptySet();
            BatchProcessor<? extends Persistent> batchProcessor = createBatchProcessor(exec, keys);
            createBatchRunner(batchProcessor).run(data);
        }
    }

    protected Set<Object> loadKeys(Execution deleteExec) {
        Execution childExec = sourceKeysSubtask.run(deleteExec.getParameters(), deleteExec);
        Set<Object> keys = (Set<Object>) childExec.getAttribute(SourceKeysTask.RESULT_KEY);
        if (keys == null) {
            throw new LmRuntimeException("Unexpected state of keys subtask. No attribute for key: "
                    + SourceKeysTask.RESULT_KEY);
        }

        return keys;
    }

    @Override
    protected void onExecFinished(Execution exec) {
        exec.getLogger().deleteExecFinished();
    }

    protected ResultIterator<?> createTargetSelect(Execution exec) {
        exec.getLogger().targetFilterApplied(targetFilter);

        ObjectSelect<?> query = ObjectSelect.query(type).where(targetFilter);
        return targetCayenneService.newContext().iterator(query);
    }

    protected BatchProcessor<? extends Persistent> createBatchProcessor(Execution exec, Set<Object> keys) {

        Index columns = Index.forLabels(DeleteSegment.TARGET_COLUMN);

        return rows -> {

            // executing in the select context
            ObjectContext context = rows.get(0).getObjectContext();
            DeleteSegment segment = new DeleteSegment(context)
                    .setTargets(DataFrame.byColumn(columns).of(Series.ofIterable(rows)))
                    .setSourceKeys(keys);
            processor.process(exec, segment);
        };
    }
}
