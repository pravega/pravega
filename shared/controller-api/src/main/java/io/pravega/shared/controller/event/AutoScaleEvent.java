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

@Data
public class AutoScaleEvent implements ControllerEvent {
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
    public String getKey() {
        return String.format("%s/%s", scope, stream);
    }
}
