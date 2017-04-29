/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.shared.controller.event;

import java.io.Serializable;

public interface ControllerEvent extends Serializable {
    /**
     * Method to get routing key for the event.
     * @return return the routing key that should be used.
     */
    String getKey();
}
