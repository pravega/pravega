/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import io.pravega.client.stream.EventPointer;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.Position;
import lombok.Data;

@Data
public class EventReadImpl<T> implements EventRead<T> {
    private final T event;
    private final Position position;
    private final EventPointer eventPointer;
    private final String checkpointName;
    
    @Override
    public boolean isCheckpoint() {
        return checkpointName != null;
    }
}
