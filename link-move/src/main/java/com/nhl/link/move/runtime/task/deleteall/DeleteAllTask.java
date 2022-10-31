package com.nhl.link.move.runtime.task.deleteall;

import com.nhl.link.move.Execution;
import com.nhl.link.move.ExecutionStats;
import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.log.LmLogger;
import com.nhl.link.move.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.move.runtime.task.BaseTask;
import com.nhl.link.move.runtime.token.ITokenManager;
import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.QueryResponse;
import org.apache.cayenne.configuration.DataNodeDescriptor;
import org.apache.cayenne.dba.DbAdapter;
import org.apache.cayenne.dba.derby.DerbyAdapter;
import org.apache.cayenne.dba.h2.H2Adapter;
import org.apache.cayenne.dba.hsqldb.HSQLDBAdapter;
import org.apache.cayenne.dba.mysql.MySQLAdapter;
import org.apache.cayenne.dba.oracle.Oracle8Adapter;
import org.apache.cayenne.dba.oracle.OracleAdapter;
import org.apache.cayenne.dba.postgres.PostgresAdapter;
import org.apache.cayenne.dba.sqlserver.SQLServerAdapter;
import org.apache.cayenne.dba.sybase.SybaseAdapter;
import org.apache.cayenne.exp.Expression;
import org.apache.cayenne.map.DataMap;
import org.apache.cayenne.map.DbEntity;
import org.apache.cayenne.query.EJBQLQuery;
import org.apache.cayenne.query.ObjectSelect;
import org.apache.cayenne.query.SQLTemplate;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;


/**
 * Task that deletes target objects that match the target filter if filter exists.
 * If no target filter provided then it deletes all rows entirely:
 * - using truncate, if database supports truncate queries.
 * - if database doesn't support truncate queries then it deletes with delete all query
 *
 * @since 3.0
 */
public class DeleteAllTask extends BaseTask {

    private static final Set<Class<?>> TRUNCATE_AWARE_ADAPTERS = Set.of(
            DerbyAdapter.class,
            PostgresAdapter.class,
            Oracle8Adapter.class,
            OracleAdapter.class,
            SQLServerAdapter.class,
            MySQLAdapter.class,
            H2Adapter.class,
            HSQLDBAdapter.class,
            SybaseAdapter.class
    );

    private final Class<?> type;
    private final Expression targetFilter;
    private final DbEntity dbEntity;
    private final boolean skipExecutionStats;
    private final ITargetCayenneService targetCayenneService;

    public DeleteAllTask(
            Class<?> type,
            Expression targetFilter,
            ITargetCayenneService targetCayenneService,
            ITokenManager tokenManager,
            DbEntity dbEntity,
            boolean skipExecutionStats,
            LmLogger logger) {

        //"deleteAll" task doesn't use extractor and doesn't support batches
        super(null, 0, tokenManager, logger);

        this.type = type;
        this.targetFilter = targetFilter;
        this.targetCayenneService = targetCayenneService;
        this.dbEntity = dbEntity;
        this.skipExecutionStats = skipExecutionStats;
    }

    @Override
    protected String createLabel() {
        return "deleteAll";
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

    private void deleteWithExpression(ObjectContext context, Execution execution) {
        StringBuilder queryBuilder = new StringBuilder("DELETE FROM " + type.getName() + " e WHERE ");

        try {
            targetFilter.appendAsEJBQL(queryBuilder, "e");
        } catch (IOException e) {
            throw new LmRuntimeException("Error while applying target filter expression", e);
        }

        EJBQLQuery delete = new EJBQLQuery(queryBuilder.toString());

        QueryResponse result = context.performGenericQuery(delete);

        incrementTaskResult(result.firstUpdateCount()[0], execution.getStats());
    }

    private void deleteAllData(ObjectContext context, Execution execution) {
        DbAdapter adapter = resolveNodeName()
                .flatMap(targetCayenneService::dbAdapter)
                .orElseThrow(() -> new LmRuntimeException("Adapter not found for dataMap: " + dbEntity.getDataMap().getName()));

        SQLTemplate query = new SQLTemplate(dbEntity.getDataMap(), "DELETE FROM $tableName", false);
        query.setParams(Map.of("tableName", adapter.getQuotingStrategy().quotedFullyQualifiedName(dbEntity)));

        TRUNCATE_AWARE_ADAPTERS.forEach(a -> query.setTemplate(a.getName(), "TRUNCATE TABLE $tableName"));

        Optional<Long> prefetchedOverallCount = prefetchCountIfRequired(adapter.unwrap(), context);

        QueryResponse result = context.performGenericQuery(query);

        long deletedCount = prefetchedOverallCount.orElse((long) result.firstUpdateCount()[0]);
        incrementTaskResult(deletedCount, execution.getStats());
    }

    private Optional<String> resolveNodeName() {
        Collection<DataNodeDescriptor> allNodeDescriptors = dbEntity.getDataMap().getDataChannelDescriptor().getNodeDescriptors();

        return allNodeDescriptors.stream()
                .filter(descriptor -> containsDataMap(descriptor, dbEntity.getDataMap()))
                .map(DataNodeDescriptor::getName)
                .findFirst();
    }

    private boolean containsDataMap(DataNodeDescriptor ds, DataMap dataMap) {
        return ds.getDataMapNames().contains(dataMap.getName());
    }

    /**
     * Truncate query does not return count of deleted rows, so additional select required to get statistics
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
