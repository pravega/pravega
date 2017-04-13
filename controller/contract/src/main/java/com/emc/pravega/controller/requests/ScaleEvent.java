/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.requests;

import lombok.Data;

@Data
public class ScaleEvent implements ControllerEvent {
    public static final byte UP = (byte) 0;
    public static final byte DOWN = (byte) 1;
    private static final long serialVersionUID = 1L;

    private final String scope;
    private final String stream;
    private final int segmentNumber;
    private final byte direction;
    private final long timestamp;
    private final int numOfSplits;
    private final boolean silent;

    @Override
    public RequestType getType() {
        return RequestType.ScaleRequest;
    }

    @Override
    public String getKey() {
        return String.format("%s/%s", scope, stream);
    }
}
