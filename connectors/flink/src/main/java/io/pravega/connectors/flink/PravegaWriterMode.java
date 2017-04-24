/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.connectors.flink;

import java.io.Serializable;

/**
 * The supported modes of operation for flink's pravega writer.
 * The different modes correspond to different guarantees for the write operations that the implementation provides.
 */
public enum PravegaWriterMode implements Serializable {
    /*
     * Any write failures will be ignored hence there could be data loss.
     */
    BEST_EFFORT,

    /*
     * The writer will guarantee that all events are persisted in pravega.
     * There could be duplicate events written though.
     */
    ATLEAST_ONCE
}
