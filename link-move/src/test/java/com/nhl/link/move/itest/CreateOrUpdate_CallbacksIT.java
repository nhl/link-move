package com.nhl.link.move.itest;

import com.nhl.link.move.Execution;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.runtime.task.MockCreateOrUpdateCallback;
import com.nhl.link.move.runtime.task.createorupdate.CreateOrUpdateStage;
import com.nhl.link.move.runtime.task.createorupdate.MockCreateOrUpdateListener;
import com.nhl.link.move.unit.LmIntegrationTest;
import com.nhl.link.move.unit.cayenne.t.Etl5t;
import org.junit.jupiter.api.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class CreateOrUpdate_CallbacksIT extends LmIntegrationTest {

    private MockCreateOrUpdateCallback extractSourceRowsCallback = mock(MockCreateOrUpdateCallback.class);
    private MockCreateOrUpdateCallback convertSourceRowsCallback = mock(MockCreateOrUpdateCallback.class);
    private MockCreateOrUpdateCallback mapSourceCallback = mock(MockCreateOrUpdateCallback.class);
    private MockCreateOrUpdateCallback matchTargetCallback1 = mock(MockCreateOrUpdateCallback.class);
    private MockCreateOrUpdateCallback matchTargetCallback2 = mock(MockCreateOrUpdateCallback.class);
    private MockCreateOrUpdateCallback mapTargetCallback = mock(MockCreateOrUpdateCallback.class);
    private MockCreateOrUpdateCallback resolveFkCallback = mock(MockCreateOrUpdateCallback.class);
    private MockCreateOrUpdateCallback mergeTargetCallback = mock(MockCreateOrUpdateCallback.class);
    private MockCreateOrUpdateCallback commitTargetCallback = mock(MockCreateOrUpdateCallback.class);

    @Test
    public void test_Listeners() {

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

    @Test
    public void test_Callbacks() {

        LmTask task = lmRuntime.service(ITaskService.class).
                createOrUpdate(Etl5t.class)
                .sourceExtractor("com/nhl/link/move/itest/etl5_to_etl5t_byid.xml")
                .stage(CreateOrUpdateStage.EXTRACT_SOURCE_ROWS, extractSourceRowsCallback)
                .stage(CreateOrUpdateStage.CONVERT_SOURCE_ROWS, convertSourceRowsCallback)
                .stage(CreateOrUpdateStage.MAP_SOURCE, mapSourceCallback)
                .stage(CreateOrUpdateStage.MATCH_TARGET, matchTargetCallback1)
                .stage(CreateOrUpdateStage.MATCH_TARGET, matchTargetCallback2)
                .stage(CreateOrUpdateStage.MAP_TARGET, mapTargetCallback)
                .stage(CreateOrUpdateStage.RESOLVE_FK_VALUES, resolveFkCallback)
                .stage(CreateOrUpdateStage.MERGE_TARGET, mergeTargetCallback)
                .stage(CreateOrUpdateStage.COMMIT_TARGET, commitTargetCallback)
                .task();

        srcEtl5().insertColumns("id", "name").values(45, "a").values(11, "b").exec();

        Execution e1 = task.run();
        assertExec(2, 2, 0, 0, e1);

        verify(extractSourceRowsCallback, times(1)).accept(any(), any());
        verify(convertSourceRowsCallback, times(1)).accept(any(), any());
        verify(mapSourceCallback, times(1)).accept(any(), any());
        verify(matchTargetCallback1, times(1)).accept(any(), any());
        verify(matchTargetCallback2, times(1)).accept(any(), any());
        verify(mapTargetCallback, times(1)).accept(any(), any());
        verify(resolveFkCallback, times(1)).accept(any(), any());
        verify(mergeTargetCallback, times(1)).accept(any(), any());
        verify(commitTargetCallback, times(1)).accept(any(), any());
    }
}
