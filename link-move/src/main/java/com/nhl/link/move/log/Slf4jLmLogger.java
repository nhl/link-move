package com.nhl.link.move.log;

import com.nhl.link.move.Execution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @since 3.0.0
 */
public class Slf4jLmLogger implements LmLogger {

    private final Logger logger;

    public Slf4jLmLogger() {
        this.logger = LoggerFactory.getLogger(LmLogger.class);
    }

    @Override
    public LmExecutionLogger executionLogger(Execution exec) {
        return new Slf4jLmExecutionLogger(logger, exec);
    }
}
