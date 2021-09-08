package com.nhl.link.move.itest;

import com.nhl.link.move.Execution;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.runtime.task.createorupdate.MockCreateListener;
import com.nhl.link.move.unit.LmIntegrationTest;
import com.nhl.link.move.unit.cayenne.t.Etl1t;
import org.junit.jupiter.api.Test;

import static org.mockito.Matchers.any;

public class Create_CallbacksIT extends LmIntegrationTest {

    @Test
    public void test_Callbacks() {

        MockCreateListener listener = new MockCreateListener();

        LmTask task = lmRuntime.service(ITaskService.class)
                .create(Etl1t.class)
                .sourceExtractor("com/nhl/link/move/itest/etl1_to_etl1t_upper.xml")
                .stageListener(listener)
                .task();

        srcEtl1().insertColumns("name", "age")
                .values("a", 3)
                .values("b", null)
                .exec();

        Execution e1 = task.run();
        assertExec(2, 2, 0, 0, e1);

        listener.verify(1).afterSourceRowsExtracted(any(), any());
        listener.verify(1).afterSourceRowsConverted(any(), any());
        listener.verify(0).afterSourceMapped(any(), any());
        listener.verify(0).afterTargetMatched(any(), any());
        listener.verify(0).afterTargetMatched2(any(), any());
        listener.verify(1).afterTargetMapped(any(), any());
        listener.verify(1).afterFksResolved(any(), any());
        listener.verify(1).afterTargetMerged(any(), any());
        listener.verify(1).afterTargetCommited(any(), any());
    }
}
