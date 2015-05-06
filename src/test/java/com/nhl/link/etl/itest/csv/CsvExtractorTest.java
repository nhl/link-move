package com.nhl.link.etl.itest.csv;

import com.nhl.link.etl.EtlTask;
import com.nhl.link.etl.Execution;
import com.nhl.link.etl.unit.EtlIntegrationTest;
import com.nhl.link.etl.unit.cayenne.t.Etl1t;
import org.apache.cayenne.query.SQLSelect;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CsvExtractorTest extends EtlIntegrationTest {

    @Test
    public void testExtractor() {
        EtlTask etlTask = etl.getTaskService().createOrUpdate(Etl1t.class)
                .sourceExtractor("com/nhl/link/etl/itest/csv/etl1_to_etl1t")
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
