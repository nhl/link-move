package com.nhl.link.move.runtime.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.nhl.link.move.Execution;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.connect.StreamConnector;
import com.nhl.link.move.connect.URLConnector;
import com.nhl.link.move.json.unit.LmJsonIntegrationTest;
import com.nhl.link.move.json.unit.cayenne.t.Etlt1;
import com.nhl.link.move.runtime.LmRuntimeBuilder;
import com.nhl.link.move.runtime.task.createorupdate.CreateOrUpdateStage;
import org.dflib.DataFrame;
import org.dflib.Exp;
import org.junit.jupiter.api.Test;

import java.net.URL;

public class CreateOrUpdate_ArrayAttributeIT extends LmJsonIntegrationTest {

    @Override
    protected LmRuntimeBuilder testRuntimeBuilder() {
        URL url = getClass()
                .getClassLoader()
                .getResource("com/nhl/link/move/json/src_json/etl1_src_array_attribute.json");

        return super
                .testRuntimeBuilder()
                .connector(StreamConnector.class, "etl_src_id", URLConnector.of(url));
    }

    @Test
    public void testCreateOrUpdate() {

        etlt1().insertColumns("id", "num_int", "string")
                .values(15, 12, null)
                .values(16, 1, "z")
                .exec();

        LmTask task = lmRuntime.getTaskService()
                .createOrUpdate(Etlt1.class)
                .sourceExtractor("com/nhl/link/move/json/extractor/etl1_src_to_etl1t_array_attribute.xml")
                .stage(CreateOrUpdateStage.EXTRACT_SOURCE_ROWS, (e, s) -> {
                    DataFrame df = s.getSourceRows().cols("strings").merge(Exp.$col("strings", JsonNode.class).mapVal(n -> {
                        if (n == null || n.size() == 0) {
                            return null;
                        }

                        StringBuilder out = new StringBuilder();
                        for (JsonNode cn : n) {
                            if (out.length() > 0) {
                                out.append(",");
                            }
                            out.append(cn.asText());
                        }

                        return out.toString();
                    }));

                    s.setSourceRows(df);
                })
                .task();

        Execution e1 = task.run();
        assertExec(3, 2, 1, 0, e1);
        etlt1().matcher().assertMatches(4);
        etlt1().matcher().eq("id", 12).andEq("num_int", 24).andEq("string", "s,t").assertOneMatch();
        etlt1().matcher().eq("id", 15).andEq("num_int", 11).andEq("string", null).assertOneMatch();
        etlt1().matcher().eq("id", -1).andEq("num_int", 4).andEq("string", null).assertOneMatch();
    }
}
