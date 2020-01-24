/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
