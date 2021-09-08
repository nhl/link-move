package com.nhl.link.move.itest;

import com.nhl.link.move.Execution;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.runtime.task.createorupdate.MockCreateOrUpdateListener;
import com.nhl.link.move.unit.LmIntegrationTest;
import com.nhl.link.move.unit.cayenne.t.Etl5t;
import org.junit.jupiter.api.Test;

import static org.mockito.Matchers.any;

public class CreateOrUpdate_CallbacksIT extends LmIntegrationTest {

    @Test
    public void test_Callbacks() {

        MockCreateOrUpdateListener listener = new MockCreateOrUpdateListener();

        LmTask task = lmRuntime.service(ITaskService.class).
                createOrUpdate(Etl5t.class)
                .sourceExtractor("com/nhl/link/move/itest/etl5_to_etl5t_byid.xml")
                .stageListener(listener)
                .task();

        srcEtl5().insertColumns("id", "name").values(45, "a").values(11, "b").exec();

        Execution e1 = task.run();
        assertExec(2, 2, 0, 0, e1);

        listener.verify(1).afterSourceRowsExtracted(any(), any());
        listener.verify(1).afterSourceRowsConverted(any(), any());
        listener.verify(1).afterSourceMapped(any(), any());
        listener.verify(1).afterTargetMatched(any(), any());
        listener.verify(1).afterTargetMatched2(any(), any());
        listener.verify(1).afterTargetMapped(any(), any());
        listener.verify(1).afterFksResolved(any(), any());
        listener.verify(1).afterTargetMerged(any(), any());
        listener.verify(1).afterTargetCommited(any(), any());
    }
}
