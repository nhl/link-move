package com.nhl.link.move.connect;

public interface Connector {

    /**
     * A method that the code using this connector should call to properly dispose of the Connector instance once it
     * is no longer needed. The default implementation does nothing.
     */
    default void shutdown() {
    }
}
