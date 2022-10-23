package com.nhl.link.move.runtime.task.delete;

import com.nhl.link.move.Execution;
import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.runtime.task.sourcekeys.SourceKeysTask;

import java.util.Set;

public class ExtractSourceKeysStage {

    public static final String SOURCE_KEYS_KEY = ExtractSourceKeysStage.class.getName() + ".SOURCE_KEYS";

    private final LmTask keysSubtask;

    public ExtractSourceKeysStage(LmTask keysSubtask) {
        this.keysSubtask = keysSubtask;
    }

    public Set<Object> extractSourceKeys(Execution exec) {
        // cache extracted source keys in the DeleteTask execution...
        return exec.computeAttributeIfAbsent(SOURCE_KEYS_KEY, k -> loadKeys(exec));
    }

    @SuppressWarnings("unchecked")
    private Set<Object> loadKeys(Execution deleteExec) {
        Execution childExec = keysSubtask.run(deleteExec.getParameters(), deleteExec);
        Set<Object> keys = (Set<Object>) childExec.getAttribute(SourceKeysTask.RESULT_KEY);
        if (keys == null) {
            throw new LmRuntimeException("Unexpected state of keys subtask. No attribute for key: "
                    + SourceKeysTask.RESULT_KEY);
        }

        return keys;
    }

}
