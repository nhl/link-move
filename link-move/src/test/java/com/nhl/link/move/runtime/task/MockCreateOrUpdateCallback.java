package com.nhl.link.move.runtime.task;

import com.nhl.link.move.Execution;
import com.nhl.link.move.runtime.task.createorupdate.CreateOrUpdateSegment;

import java.util.function.BiConsumer;

public class MockCreateOrUpdateCallback implements BiConsumer<Execution, CreateOrUpdateSegment> {
    @Override
    public void accept(Execution execution, CreateOrUpdateSegment createOrUpdateSegment) {
        execution.getStats();
        createOrUpdateSegment.getContext();
    }
}
