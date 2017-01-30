package com.emc.pravega.controller.requests;

import java.io.Serializable;

public interface ControllerRequest extends Serializable {
    RequestType getType();

    public enum RequestType {
        ScaleRequest
    }
}
