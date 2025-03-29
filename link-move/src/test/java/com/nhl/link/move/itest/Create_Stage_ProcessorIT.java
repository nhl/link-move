package com.nhl.link.move.itest;

import com.nhl.link.move.Execution;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.runtime.task.create.CreateSegment;
import com.nhl.link.move.runtime.task.create.CreateStage;
import com.nhl.link.move.unit.LmIntegrationTest;
import com.nhl.link.move.unit.cayenne.t.Etl1t;
import org.dflib.Exp;
import org.junit.jupiter.api.Test;

public class Create_Stage_ProcessorIT extends LmIntegrationTest {

    @Test
    public void test() {

        srcEtl1().insertColumns("name")
                .values("a")
                .values("b")
                .exec();

        LmTask task = lmRuntime.service(ITaskService.class)
                .create(Etl1t.class)
                .sourceExtractor("com/nhl/link/move/itest/etl1_to_etl1t_upper_partial.xml")
                .stage(CreateStage.EXTRACT_SOURCE_ROWS, df -> df.cols("NAME").merge(Exp.$col("NAME").mapVal(s -> s + "_esr")))
                .stage(CreateStage.CONVERT_SOURCE_ROWS, df -> df.cols("db:name").merge(Exp.$col("db:name").mapVal(s -> s + "_csr")))
                .stage(CreateStage.MAP_TARGET, df -> df.cols("db:name").merge(Exp.$col("db:name").mapVal(s -> s + "_mt1")))
                .stage(CreateStage.RESOLVE_FK_VALUES, df -> df.cols("db:name").merge(Exp.$col("db:name").mapVal(s -> s + "_rfv")))
                .stage(CreateStage.MERGE_TARGET, df -> {
                    df.getColumn(CreateSegment.TARGET_COLUMN).forEach(o -> ((Etl1t) o).setDescription("mt2"));
                    return df;
                })
                .task();

        Execution e1 = task.run();
        assertExec(2, 2, 0, 0, e1);
        etl1t().matcher().assertMatches(2);
        etl1t().matcher().eq("name", "a_esr_csr_mt1_rfv").andEq("description", "mt2").assertOneMatch();
        etl1t().matcher().eq("name", "b_esr_csr_mt1_rfv").andEq("description", "mt2").assertOneMatch();

    }
}
