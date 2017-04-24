/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.common.cluster;

/**
 * Cluster listener.
 */
public interface ClusterListener {

    enum EventType {
        HOST_ADDED,
        HOST_REMOVED,
        ERROR
    }

    /**
     * Method invoked on cluster Event.
     *
     * @param type Event type.
     * @param host Host added/removed, in case of an ERROR a null host value is passed.
     */
    public void onEvent(final EventType type, final Host host);

}
