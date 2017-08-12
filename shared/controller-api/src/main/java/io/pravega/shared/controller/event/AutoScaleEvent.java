/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.controller.event;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = false)
public class AutoScaleEvent extends StreamEvent {
    public static final byte UP = (byte) 0;
    public static final byte DOWN = (byte) 1;
    private static final long serialVersionUID = 1L;

    private final int segmentNumber;
    private final byte direction;
    private final long timestamp;
    private final int numOfSplits;
    private final boolean silent;

    public AutoScaleEvent(String scope, String stream, int segmentNumber, byte direction, long timestamp, int numOfSplits, boolean silent) {
        super(scope, stream);
        this.segmentNumber = segmentNumber;
        this.direction = direction;
        this.timestamp = timestamp;
        this.numOfSplits = numOfSplits;
        this.silent = silent;
    }
}
