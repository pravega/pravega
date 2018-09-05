/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.tables;

import com.google.common.base.Preconditions;
import io.pravega.common.io.SerializationException;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.BitConverter;
import java.io.IOException;
import java.io.InputStream;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * Serializes Key-Values to byte arrays.
 */
final class EntrySerializer {
    static final int HEADER_LENGTH = Integer.BYTES * 2;
    static final int MAX_KEY_LENGTH = 8 * 1024; // 8KB
    static final int MAX_SERIALIZATION_LENGTH = 1024 * 1024; // 1MB

    static byte[] serialize(@NonNull byte[] key, byte[] value) {
        Preconditions.checkArgument(key.length <= MAX_KEY_LENGTH, "Key too large.");
        int valueLength = value == null ? 0 : value.length;
        int serializationLength = HEADER_LENGTH + key.length + valueLength;
        Preconditions.checkArgument(serializationLength <= MAX_SERIALIZATION_LENGTH, "Key+Value serialization too large.");

        byte[] result = new byte[serializationLength];
        int offset = 0;
        offset += BitConverter.writeInt(result, offset, key.length);
        offset += BitConverter.writeInt(result, offset, valueLength);
        System.arraycopy(key, 0, result, offset, key.length);
        if (valueLength > 0) {
            offset += key.length;
            System.arraycopy(value, 0, result, offset, value.length);
        }
        return result;
    }

    static Header readHeader(@NonNull ArrayView input) throws IOException {
        int keyLength = BitConverter.readInt(input, 0);
        int valueLength = BitConverter.readInt(input, Integer.BYTES);
        if (keyLength <= 0 || keyLength > MAX_KEY_LENGTH || valueLength < 0 || keyLength + valueLength > MAX_SERIALIZATION_LENGTH) {
            throw new SerializationException(String.format("Read header with invalid data. KeyLength=%s, ValueLength=%s", keyLength, valueLength));
        }

        return new Header(keyLength, valueLength);
    }

    static Header readHeader(@NonNull InputStream input) throws IOException {
        int keyLength = BitConverter.readInt(input);
        int valueLength = BitConverter.readInt(input);
        if (keyLength <= 0 || keyLength > MAX_KEY_LENGTH || valueLength < 0 || keyLength + valueLength > MAX_SERIALIZATION_LENGTH) {
            throw new SerializationException(String.format("Read header with invalid data. KeyLength=%s, ValueLength=%s", keyLength, valueLength));
        }

        return new Header(keyLength, valueLength);
    }

    @Getter
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    static class Header {
        private final int keyLength;
        private final int valueLength;

        int getKeyOffset() {
            return HEADER_LENGTH;
        }

        int getValueOffset() {
            return HEADER_LENGTH + this.keyLength;
        }

        int getTotalLength() {
            return HEADER_LENGTH + this.keyLength + this.valueLength;
        }

        @Override
        public String toString() {
            return String.format("Length: Key=%s, Value=%s, Total=%s", this.keyLength, this.valueLength, getTotalLength());
        }
    }
}