package com.nhl.link.move.runtime.task.deleteall;

import com.nhl.link.move.Execution;
import com.nhl.link.move.ExecutionStats;
import com.nhl.link.move.log.LmLogger;
import com.nhl.link.move.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.move.runtime.task.BaseTask;
import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.Persistent;
import org.apache.cayenne.QueryResult;
import org.apache.cayenne.UpdateResult;
import org.apache.cayenne.dba.DbAdapter;
import org.apache.cayenne.dba.derby.DerbyAdapter;
import org.apache.cayenne.dba.h2.H2Adapter;
import org.apache.cayenne.dba.hsqldb.HSQLDBAdapter;
import org.apache.cayenne.dba.mysql.MySQLAdapter;
import org.apache.cayenne.dba.oracle.OracleAdapter;
import org.apache.cayenne.dba.postgres.PostgresAdapter;
import org.apache.cayenne.dba.sqlserver.SQLServerAdapter;
import org.apache.cayenne.dba.sybase.SybaseAdapter;
import org.apache.cayenne.access.translator.select.LmDeleteTranslator;
import org.apache.cayenne.exp.Expression;
import org.apache.cayenne.map.DbEntity;
import org.apache.cayenne.query.ObjectSelect;
import org.apache.cayenne.query.SQLTemplate;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;


/**
 * Task that deletes target objects that match the target filter if filter exists.
 * If no target filter provided then it deletes all rows entirely:
 * - using truncate, if database supports truncate queries.
 * - if database doesn't support truncate queries then it deletes with delete all query
 *
 * @since 3.0.0
 */
public class DeleteAllTask extends BaseTask {

    private static final Set<Class<?>> TRUNCATE_AWARE_ADAPTERS = Set.of(
            DerbyAdapter.class,
            PostgresAdapter.class,
            OracleAdapter.class,
            SQLServerAdapter.class,
            MySQLAdapter.class,
            H2Adapter.class,
            HSQLDBAdapter.class,
            SybaseAdapter.class
    );

    private final Class<? extends Persistent> type;
    private final Expression targetFilter;
    private final DbEntity dbEntity;
    private final boolean skipExecutionStats;
    private final ITargetCayenneService targetCayenneService;

    public DeleteAllTask(
            Class<?> type,
            Expression targetFilter,
            ITargetCayenneService targetCayenneService,
            DbEntity dbEntity,
            boolean skipExecutionStats,
            LmLogger logger) {

        //"deleteAll" task doesn't use extractor and doesn't support batches
        super(null, 0, logger);

        this.type = type.asSubclass(Persistent.class);
        this.targetFilter = targetFilter;
        this.targetCayenneService = targetCayenneService;
        this.dbEntity = dbEntity;
        this.skipExecutionStats = skipExecutionStats;
    }

    @Override
    protected String createLabel() {
        return "delete-all";
    }

    @Override
    protected void doRun(Execution exec) {
        ObjectContext context = targetCayenneService.newContext();

        if (targetFilter != null) {
            deleteWithExpression(context, exec);
        } else {
            deleteAllData(context, exec);
        }

        context.commitChanges();
    }

    @Override
    protected void onExecFinished(Execution exec) {
        exec.getLogger().deleteExecFinished();
    }

    private void deleteWithExpression(ObjectContext context, Execution exec) {

        exec.getLogger().targetFilterApplied(targetFilter);

        // Cayenne 5.0 has no bulk delete query driven by an Expression (EJBQL "DELETE" is gone), so we use
        // Cayenne's own SQL translator to render the filter as a "DELETE ... WHERE" and run it as a SQLTemplate
        DbAdapter adapter = targetCayenneService.dbAdapter(dbEntity.getDataMap().getName());
        SQLTemplate delete = LmDeleteTranslator.deleteTemplate(
                ObjectSelect.query(type).where(targetFilter),
                adapter,
                targetCayenneService.entityResolver());

        List<QueryResult> result = context.execute(delete);

        incrementTaskResult(((UpdateResult) result.getFirst()).count(), exec.getStats());
    }

    private void deleteAllData(ObjectContext context, Execution execution) {
        DbAdapter adapter = targetCayenneService.dbAdapter(dbEntity.getDataMap().getName());

        SQLTemplate query = new SQLTemplate(dbEntity.getDataMap(), "DELETE FROM $tableName", false);
        query.setParams(Map.of("tableName", adapter.getQuotingStrategy(dbEntity)
                .quotedFQN(dbEntity.getCatalog(), dbEntity.getSchema(), dbEntity.getName())));

        TRUNCATE_AWARE_ADAPTERS.forEach(a -> query.setTemplate(a.getName(), "TRUNCATE TABLE $tableName"));

        Optional<Long> prefetchedOverallCount = prefetchCountIfRequired(adapter.unwrap(), context);

        List<QueryResult> result = context.execute(query);

        long deletedCount = prefetchedOverallCount.orElse((long) ((UpdateResult) result.getFirst()).count());
        incrementTaskResult(deletedCount, execution.getStats());
    }

    /**
     * Truncate query does not return count of deleted rows, so an additional select is required to get statistics
     */
    private Optional<Long> prefetchCountIfRequired(DbAdapter adapter, ObjectContext context) {
        if (!skipExecutionStats && TRUNCATE_AWARE_ADAPTERS.contains(adapter.getClass())) {
            return Optional.of(ObjectSelect.query(type).count().select(context).get(0));
        } else {
            return Optional.empty();
        }
    }

    private static void incrementTaskResult(long deletedCount, ExecutionStats stats) {
        stats.incrementExtracted(deletedCount);
        stats.incrementDeleted(deletedCount);
    }
}
