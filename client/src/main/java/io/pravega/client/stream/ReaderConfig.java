/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream;

import java.io.Serializable;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ReaderConfig implements Serializable {

    private static final long serialVersionUID = 1L;
    private final long initialAllocationDelay;
    private final boolean disableTimeWindows;
    private final int bufferSize;
    
    public static class ReaderConfigBuilder {
        private long initialAllocationDelay = 0;
        private boolean disableTimeWindows = false;
        private int bufferSize = 1024 * 1024;
    }
    
}
