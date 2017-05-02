/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.server.controller.service.store.stream.tables;

/**
 * This is used to represent the state of the Stream.
 */
public enum State {
    UNKNOWN,
    CREATING,
    ACTIVE,
    SEALED
}
