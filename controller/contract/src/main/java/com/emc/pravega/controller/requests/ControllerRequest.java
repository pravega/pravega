/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.requests;

import java.io.Serializable;

public interface ControllerRequest extends Serializable {
    RequestType getType();

    String getKey();

    enum RequestType {
        ScaleRequest
    }
}
