package com.nhl.link.move.runtime.task.delete;

import com.nhl.dflib.DataFrame;
import com.nhl.dflib.Index;
import com.nhl.dflib.Series;
import com.nhl.link.move.Execution;
import com.nhl.link.move.batch.BatchProcessor;
import com.nhl.link.move.extractor.model.ExtractorName;
import com.nhl.link.move.log.LmLogger;
import com.nhl.link.move.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.move.runtime.task.BaseTask;
import com.nhl.link.move.runtime.token.ITokenManager;
import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.Persistent;
import org.apache.cayenne.ResultIterator;
import org.apache.cayenne.exp.Expression;
import org.apache.cayenne.query.ObjectSelect;

/**
 * A task that allows to delete target objects not present in the source.
 *
 * @since 1.3
 */
public class DeleteTask extends BaseTask {

    private final Class<?> type;
    private final Expression targetFilter;
    private final DeleteSegmentProcessor processor;
    private final ITargetCayenneService targetCayenneService;

    public DeleteTask(
            ExtractorName extractorName,
            int batchSize,
            Class<?> type,
            Expression targetFilter,
            ITargetCayenneService targetCayenneService,
            ITokenManager tokenManager,
            DeleteSegmentProcessor processor,
            LmLogger logger) {

        super(extractorName, batchSize, tokenManager, logger);

        this.type = type;
        this.targetFilter = targetFilter;
        this.targetCayenneService = targetCayenneService;
        this.processor = processor;
    }

    @Override
    protected String createLabel() {
        return "delete";
    }

    @Override
    protected void doRun(Execution exec) {

        BatchProcessor<? extends Persistent> batchProcessor = createBatchProcessor(exec);

        try (ResultIterator data = createTargetSelect()) {
            createBatchRunner(batchProcessor).run(data);
        }
    }

    protected ResultIterator<?> createTargetSelect() {
        ObjectSelect<?> query = ObjectSelect.query(type).where(targetFilter);
        return targetCayenneService.newContext().iterator(query);
    }

    protected BatchProcessor<? extends Persistent> createBatchProcessor(Execution execution) {

        Index columns = Index.forLabels(DeleteSegment.TARGET_COLUMN);

        return rows -> {

            // executing in the select context
            ObjectContext context = rows.get(0).getObjectContext();
            DataFrame df = DataFrame.newFrame(columns).columns(Series.forData(rows));
            processor.process(execution, new DeleteSegment(context, df));
        };
    }
}
