package com.emc.pravega.controller.requests;

import lombok.Data;

import java.io.Serializable;

@Data
public class ScaleRequest implements ControllerRequest {
    private final String scope;
    private final String stream;
    private final int segmentNumber;
    private final boolean up;
    private final long timestamp;
    private final int numOfSplits;

    @Override
    public RequestType getType() {
        return RequestType.ScaleRequest;
    }
}
