package com.nhl.link.move.log;

import com.nhl.link.move.Execution;

/**
 * @since 3.0.0
 */
public interface LmLogger {

    LmExecutionLogger executionLogger(Execution exec);
}
