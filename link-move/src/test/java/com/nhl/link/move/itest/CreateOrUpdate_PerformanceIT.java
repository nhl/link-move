package com.nhl.link.move.itest;

import com.nhl.link.move.Execution;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.unit.LmIntegrationTest;
import com.nhl.link.move.unit.cayenne.t.Etl3t;
import org.apache.cayenne.DataChannel;
import org.apache.cayenne.DataChannelFilter;
import org.apache.cayenne.DataChannelFilterChain;
import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.QueryResponse;
import org.apache.cayenne.graph.GraphDiff;
import org.apache.cayenne.map.EntityResolver;
import org.apache.cayenne.query.ObjectSelect;
import org.apache.cayenne.query.Query;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

public class CreateOrUpdate_PerformanceIT extends LmIntegrationTest {

    private static QueryCapture QUERY_CAPTURE;

    @BeforeClass
    public static void initQueryCapture() {
        QUERY_CAPTURE = new QueryCapture();
        targetStack.runtime().getDataDomain().addFilter(QUERY_CAPTURE);
    }

    @Before
    public void resetQueryCapture() {
        QUERY_CAPTURE.queries.clear();
    }

    @Test
    public void test_SyncFk_QueryCounts() {

        LmTask task = etl
                .service(ITaskService.class)
                .createOrUpdate(Etl3t.class)
                .sourceExtractor("com/nhl/link/move/itest/etl3_to_etl3t.xml")
                .matchBy(Etl3t.NAME)
                .task();

        srcRunSql("INSERT INTO utest.etl2 (ID, ADDRESS, NAME) VALUES (34, 'Address1', '2Name1')");
        srcRunSql("INSERT INTO utest.etl5 (ID, NAME) VALUES (17, '5Name1')");
        srcRunSql("INSERT INTO utest.etl3 (E2_ID, E5_ID, NAME, PHONE_NUMBER) VALUES (34, 17, '3Name1', '3PHONE1')");
        srcRunSql("INSERT INTO utest.etl3 (E2_ID, E5_ID, NAME, PHONE_NUMBER) VALUES (34, 17, '3Name2', '3PHONE2')");
        srcRunSql("INSERT INTO utest.etl3 (E2_ID, E5_ID, NAME, PHONE_NUMBER) VALUES (34, 17, '3Name3', '3PHONE3')");

        targetRunSql("INSERT INTO utest.etl2t (ID, ADDRESS, NAME) VALUES (34, 'Address1', '2Name1')");
        targetRunSql("INSERT INTO utest.etl5t (ID, NAME) VALUES (17, '5Name1')");
        targetRunSql("INSERT INTO utest.etl3t (E2_ID, E5_ID, NAME, PHONE_NUMBER) VALUES (34, 17, '3Name3', '3PHONEXX')");


        Execution e1 = task.run();
        assertExec(3, 2, 1, 0, e1);

        EntityResolver resolver = targetContext.getEntityResolver();
        List<String> resolvedEntities = QUERY_CAPTURE.getOfType(ObjectSelect.class)
                .map(q -> q.getMetaData(resolver).getClassDescriptor().getEntity().getName())
                .collect(toList());

        assertEquals("Each id (including root target) must have been resolved only once. Instead got " + resolvedEntities,
                3, resolvedEntities.size());
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
