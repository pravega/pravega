/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.utils;

import io.pravega.client.stream.Serializer;

import java.nio.ByteBuffer;

public final class IntegerSerializer implements Serializer<Integer> {
    @Override
    public ByteBuffer serialize(Integer value) {
        ByteBuffer result = ByteBuffer.allocate(4).putInt(value);
        result.rewind();
        return result;
    }

    @Override
    public Integer deserialize(ByteBuffer serializedValue) {
        return serializedValue.getInt();
    }
}
