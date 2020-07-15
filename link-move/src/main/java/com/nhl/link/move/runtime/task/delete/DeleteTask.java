package com.nhl.link.move.runtime.task.delete;

import com.nhl.dflib.DataFrame;
import com.nhl.dflib.Index;
import com.nhl.dflib.Series;
import com.nhl.link.move.Execution;
import com.nhl.link.move.batch.BatchProcessor;
import com.nhl.link.move.batch.BatchRunner;
import com.nhl.link.move.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.move.runtime.task.BaseTask;
import com.nhl.link.move.runtime.token.ITokenManager;
import org.apache.cayenne.DataObject;
import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.ResultIterator;
import org.apache.cayenne.exp.Expression;
import org.apache.cayenne.query.ObjectSelect;

import java.util.Map;

/**
 * A task that allows to delete target objects not present in the source.
 *
 * @since 1.3
 */
public class DeleteTask<T extends DataObject> extends BaseTask {

    String extractorName;
    int batchSize;
    Class<T> type;
    Expression targetFilter;

    private DeleteSegmentProcessor<T> processor;
    private ITargetCayenneService targetCayenneService;

    public DeleteTask(
            String extractorName,
            int batchSize,
            Class<T> type,
            Expression targetFilter,
            ITargetCayenneService targetCayenneService,
            ITokenManager tokenManager,
            DeleteSegmentProcessor<T> processor) {

        super(tokenManager);

        this.extractorName = extractorName;
        this.batchSize = batchSize;
        this.type = type;
        this.targetFilter = targetFilter;
        this.targetCayenneService = targetCayenneService;
        this.processor = processor;
    }

    @Override
    protected Execution doRun(Map<String, ?> params) {

        try (Execution execution = new Execution("DeleteTask:" + extractorName, params);) {

            BatchProcessor<T> batchProcessor = createBatchProcessor(execution);

            try (ResultIterator<T> data = createTargetSelect()) {
                BatchRunner.create(batchProcessor).withBatchSize(batchSize).run(data);
            }

            return execution;
        }
    }

    protected ResultIterator<T> createTargetSelect() {
        ObjectSelect<T> query = ObjectSelect.query(type).where(targetFilter);
        return targetCayenneService.newContext().iterator(query);
    }

    protected BatchProcessor<T> createBatchProcessor(Execution execution) {

        Index columns = Index.forLabels(DeleteSegment.TARGET_COLUMN);

        return rows -> {

            // executing in the select context..
            ObjectContext context = rows.get(0).getObjectContext();
            DataFrame df = DataFrame.newFrame(columns).columns(Series.forData(rows));
            processor.process(execution, new DeleteSegment<>(context, df));
        };
    }
}
