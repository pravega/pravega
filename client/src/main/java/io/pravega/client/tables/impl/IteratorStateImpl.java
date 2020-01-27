/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.tables.impl;

import io.netty.buffer.ByteBuf;
import lombok.Data;

/**
 * Implementation of {@link KeyVersion}.
 */
@Data
public class IteratorStateImpl implements IteratorState {

    private final ByteBuf token;

    @Override
    public ByteBuf toBytes() {
        return token;
    }
}
