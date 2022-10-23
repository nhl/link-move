package com.nhl.link.move.runtime.task.delete;

import com.nhl.link.move.Execution;
import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.runtime.task.sourcekeys.SourceKeysTask;

import java.util.Set;

public class ExtractSourceKeysStage {

    public static final String SOURCE_KEYS_KEY = ExtractSourceKeysStage.class.getName() + ".SOURCE_KEYS";

    private LmTask keysSubtask;

    public ExtractSourceKeysStage(LmTask keysSubtask) {
        this.keysSubtask = keysSubtask;
    }

    public Set<Object> extractSourceKeys(Execution exec) {
        // cache keys in the DeleteTask execution...
        Set<Object> keys = (Set<Object>) exec.getAttribute(SOURCE_KEYS_KEY);
        if (keys == null) {
            keys = loadKeys(exec);
            exec.setAttribute(SOURCE_KEYS_KEY, keys);
        }

        return keys;
    }

    @SuppressWarnings("unchecked")
    private Set<Object> loadKeys(Execution parentExec) {
        Execution childExec = keysSubtask.run(parentExec.getParameters(), parentExec);
        Set<Object> keys = (Set<Object>) childExec.getAttribute(SourceKeysTask.RESULT_KEY);
        if (keys == null) {
            throw new LmRuntimeException("Unexpected state of keys subtask. No attribute for key: "
                    + SourceKeysTask.RESULT_KEY);
        }

        return keys;
    }

}
