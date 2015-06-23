package com.nhl.link.move.itest;

import com.nhl.link.move.EtlTask;
import com.nhl.link.move.Execution;
import com.nhl.link.move.unit.EtlIntegrationTest;
import com.nhl.link.move.unit.cayenne.t.Etl1t;

import org.apache.cayenne.query.SQLSelect;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CsvExtractor_CreateOrUpdateIT extends EtlIntegrationTest {

    @Test
    public void testExtractor() {
        EtlTask etlTask = etl.getTaskService().createOrUpdate(Etl1t.class)
                .sourceExtractor("com/nhl/link/move/itest/csv/etl1_to_etl1t")
                .matchBy(Etl1t.NAME)
                .task();

        Execution execution = etlTask.run();
        assertExec(2, 2, 0, 0, execution);
        assertEquals(
                "Enjoy the little things in life, for one day you`ll look back and realize they were big things.",
                SQLSelect.scalarQuery(
                        String.class, "SELECT description FROM utest.etl1t WHERE name = 'Vonnegut, Kurt'"
                ).selectOne(targetContext)
        );
        assertEquals(
                "He who does not value life does not deserve it.",
                SQLSelect.scalarQuery(
                        String.class, "SELECT description FROM utest.etl1t WHERE name = 'Leonardo da Vinci'"
                ).selectOne(targetContext)
        );
    }

}
