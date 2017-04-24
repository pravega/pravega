/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.connectors.flink;

import java.io.Serializable;

/**
 * The event router which is used to extract the routing key for pravega from the event.
 *
 * @param <T> The type of the event.
 */
public interface PravegaEventRouter<T> extends Serializable {
    /**
     * Fetch the routing key for the given event.
     *
     * @param event The type of the event.
     * @return  The routing key which will be used by the pravega writer.
     */
    String getRoutingKey(T event);
}
