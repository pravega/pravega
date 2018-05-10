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

import io.pravega.client.stream.StreamCut;
import java.nio.ByteBuffer;
import lombok.EqualsAndHashCode;

/**
 * This class represents an unbounded StreamCut. This is used when the user wants to refer to the current HEAD
 * of the stream or the current TAIL of the stream.
 */
@EqualsAndHashCode
public final class UnboundedStreamCut implements StreamCut {
    private static final long serialVersionUID = 1L;

    @Override
    public ByteBuffer toBytes() {
        return ByteBuffer.allocate(0);
    }
    
    @Override
    public StreamCutInternal asImpl() {
        return null;
    }
}
