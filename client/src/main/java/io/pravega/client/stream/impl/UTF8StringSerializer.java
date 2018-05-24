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

import io.pravega.client.stream.Serializer;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * An implementation of {@link Serializer} that converts UTF-8 strings.
 * Note that this is incompatible with {@link JavaSerializer} of String.
 */
public class UTF8StringSerializer implements Serializer<String>, Serializable {
    private static final long serialVersionUID = 1L;
    @Override
    public ByteBuffer serialize(String value) {
        return ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public String deserialize(ByteBuffer serializedValue) {
        return StandardCharsets.UTF_8.decode(serializedValue).toString();
    }
}
