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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.client.stream.Serializer;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nullable;
import org.apache.commons.lang3.SerializationException;

/**
 * Serializer for Key-Value Table Key Families.
 */
public class KeyFamilySerializer implements Serializer<String> {
    @VisibleForTesting
    static final int MAX_KEY_FAMILY_LENGTH = 8191; // It can't be longer than TableSegment.MAX_KEY_LENGTH
    @VisibleForTesting
    static final int PREFIX_LENGTH = 2; // [0, 65535]. First 3 bits will be empty.
    @VisibleForTesting
    static final Charset ENCODING = StandardCharsets.UTF_8;

    @Override
    public ByteBuffer serialize(@Nullable String keyFamily) {
        ByteBuffer result;
        if (keyFamily == null || keyFamily.length() == 0) {
            result = ByteBuffer.allocate(PREFIX_LENGTH); // Prefix is 00, which is the length of the Key Family
        } else {
            ByteBuffer s = ENCODING.encode(keyFamily);
            Preconditions.checkArgument(s.remaining() <= MAX_KEY_FAMILY_LENGTH,
                    "KeyFamily must have a %s encoding length of at most %s bytes; actual %s.", ENCODING.name(), MAX_KEY_FAMILY_LENGTH, s.remaining());
            result = ByteBuffer.allocate(PREFIX_LENGTH + s.remaining());
            result.putShort((short) s.remaining());
            result.put(s);
        }

        return (ByteBuffer) result.position(0);
    }

    @Override
    public String deserialize(ByteBuffer serializedValue) {
        if (serializedValue.remaining() < PREFIX_LENGTH) {
            throw new SerializationException(String.format("At least %d bytes required, given %d.", PREFIX_LENGTH, serializedValue.remaining()));
        }

        int kfLength = serializedValue.getShort();
        if (kfLength < 0 || kfLength > MAX_KEY_FAMILY_LENGTH) {
            throw new SerializationException(String.format("Invalid Key Family Length. Expected between %d and %d, read %d.",
                    0, MAX_KEY_FAMILY_LENGTH, kfLength));
        } else if (serializedValue.remaining() < kfLength) {
            throw new SerializationException(String.format(
                    "Insufficient bytes remaining in buffer. Expected at least %d, actual %d.", kfLength, serializedValue.remaining()));
        } else if (kfLength == 0) {
            // No Key Family
            return null;
        } else {
            String result = ENCODING.decode((ByteBuffer) serializedValue.slice().limit(kfLength)).toString();
            serializedValue.position(serializedValue.position() + kfLength);
            return result;
        }
    }
}
