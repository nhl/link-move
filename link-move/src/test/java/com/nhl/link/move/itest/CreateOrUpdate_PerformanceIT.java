package com.nhl.link.move.itest;

import com.nhl.link.move.Execution;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.unit.LmIntegrationTest;
import com.nhl.link.move.unit.cayenne.t.Etl3t;
import org.apache.cayenne.*;
import org.apache.cayenne.graph.GraphDiff;
import org.apache.cayenne.map.EntityResolver;
import org.apache.cayenne.query.ObjectSelect;
import org.apache.cayenne.query.Query;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class CreateOrUpdate_PerformanceIT extends LmIntegrationTest {

    private static final QueryCapture QUERY_CAPTURE = new QueryCapture();

    @BeforeAll
    public static void initQueryCapture() {
        targetCayenne.getRuntime().getDataDomain().addFilter(QUERY_CAPTURE);
    }

    @BeforeEach
    public void resetQueryCapture() {
        QUERY_CAPTURE.queries.clear();
    }

    @Test
    public void test_SyncFk_QueryCounts() {

        LmTask task = lmRuntime
                .service(ITaskService.class)
                .createOrUpdate(Etl3t.class)
                .sourceExtractor("com/nhl/link/move/itest/etl3_to_etl3t.xml")
                .matchBy(Etl3t.NAME)
                .task();

        srcEtl2().insertColumns("id", "address", "name").values(34, "Address1", "2Name1").exec();
        srcEtl5().insertColumns("id", "name").values(17, "5Name1").exec();
        srcEtl3().insertColumns("e2_id", "e5_id", "name", "phone_number")
                .values(34, 17, "3Name1", "3PHONE1")
                .values(34, 17, "3Name2", "3PHONE2")
                .values(34, 17, "3Name3", "3PHONE3")
                .exec();

        etl2t().insertColumns("ID", "ADDRESS", "NAME").values(34, "Address1", "2Name1").exec();
        etl5t().insertColumns("ID", "NAME").values(17, "5Name1").exec();
        etl3t().insertColumns("E2_ID", "E5_ID", "NAME", "PHONE_NUMBER").values(34, 17, "3Name3", "3PHONEXX").exec();

        Execution e1 = task.run();
        assertExec(3, 2, 1, 0, e1);

        EntityResolver resolver = targetContext.getEntityResolver();
        List<String> resolvedEntities = QUERY_CAPTURE.getOfType(ObjectSelect.class)
                .map(q -> q.getMetaData(resolver).getClassDescriptor().getEntity().getName())
                .collect(toList());

        assertEquals(3, resolvedEntities.size(),
                () -> "Each id (including root target) must have been resolved only once. Instead got " + resolvedEntities);
        Set<String> expectedRoots = new HashSet<>(asList("Etl3t", "Etl2t", "Etl5t"));
        assertEquals(expectedRoots, new HashSet<>(resolvedEntities));
    }

    static class QueryCapture implements DataChannelFilter {

        private Collection<Query> queries;

        public <T extends Query> Stream<T> getOfType(Class<T> type) {
            return queries.stream()
                    .filter(q -> type.isAssignableFrom(q.getClass()))
                    .map(q -> (T) q);
        }

        @Override
        public void init(DataChannel channel) {
            queries = new ArrayList<>();
        }

        @Override
        public QueryResponse onQuery(ObjectContext originatingContext, Query query, DataChannelFilterChain filterChain) {
            queries.add(query);
            return filterChain.onQuery(originatingContext, query);
        }

        @Override
        public GraphDiff onSync(ObjectContext originatingContext, GraphDiff changes, int syncType, DataChannelFilterChain filterChain) {
            return filterChain.onSync(originatingContext, changes, syncType);
        }
    }
}
