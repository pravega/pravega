/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.requests;

import java.io.Serializable;

public interface ControllerEvent extends Serializable {
    String getKey();
}
