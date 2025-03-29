package com.nhl.link.move.itest;

import com.nhl.link.move.Execution;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.runtime.task.createorupdate.CreateOrUpdateSegment;
import com.nhl.link.move.runtime.task.createorupdate.CreateOrUpdateStage;
import com.nhl.link.move.unit.LmIntegrationTest;
import com.nhl.link.move.unit.cayenne.t.Etl1t;
import org.dflib.Exp;
import org.junit.jupiter.api.Test;

public class CreateOrUpdate_Stage_ProcessorIT extends LmIntegrationTest {

    @Test
    public void test() {

        srcEtl1().insertColumns("name", "age")
                .values("a", 3)
                .values("b", 4)
                .exec();

        etl1t().insertColumns("name", "age")
                .values("A", 3)
                .exec();

        LmTask task = lmRuntime.service(ITaskService.class)
                .createOrUpdate(Etl1t.class)
                .sourceExtractor("com/nhl/link/move/itest/etl1_to_etl1t_upper_partial.xml")
                .matchBy(Etl1t.AGE)
                .stage(CreateOrUpdateStage.EXTRACT_SOURCE_ROWS, df -> df.cols("NAME").merge(Exp.$col("NAME").mapVal(s -> s + "_esr")))
                .stage(CreateOrUpdateStage.CONVERT_SOURCE_ROWS, df -> df.cols("db:name").merge(Exp.$col("db:name").mapVal(s -> s + "_csr")))
                .stage(CreateOrUpdateStage.MAP_SOURCE, df -> df.cols("db:name").merge(Exp.$col("db:name").mapVal(s -> s + "_ms")))
                .stage(CreateOrUpdateStage.MATCH_TARGET, df -> {
                    df.getColumn(CreateOrUpdateSegment.TARGET_COLUMN).forEach(o -> ((Etl1t) o).setDescription("mt1"));
                    return df;
                })
                .stage(CreateOrUpdateStage.MAP_TARGET, df -> df.cols("db:name").merge(Exp.$col("db:name").mapVal(s -> s + "_mt2")))
                .stage(CreateOrUpdateStage.RESOLVE_FK_VALUES, df -> df.cols("db:name").merge(Exp.$col("db:name").mapVal(s -> s + "_rfv")))
                .stage(CreateOrUpdateStage.MERGE_TARGET, df -> {
                    df.getColumn(CreateOrUpdateSegment.TARGET_COLUMN).forEach(o -> ((Etl1t) o).setDescription(((Etl1t) o).getDescription() + "_mt3"));
                    return df;
                })
                .task();

        Execution e1 = task.run();
        assertExec(2, 1, 1, 0, e1);
        etl1t().matcher().assertMatches(2);
        etl1t().matcher().eq("name", "a_esr_csr_ms_mt2_rfv").andEq("description", "mt1_mt3").assertOneMatch();
        etl1t().matcher().eq("name", "b_esr_csr_ms_mt2_rfv").andEq("description", "null_mt3").assertOneMatch();

    }
}
