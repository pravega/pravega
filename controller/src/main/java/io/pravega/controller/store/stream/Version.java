/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import io.pravega.common.ObjectBuilder;
import io.pravega.controller.store.stream.tables.serializers.IntVersionSerializer;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;

import java.io.IOException;

public interface Version {
    IntVersion asIntVersion();

    byte[] toByteArray();

    @Data
    @Builder
    class IntVersion implements Version {
        public static final IntVersionSerializer SERIALIZER = new IntVersionSerializer();
        public static final IntVersion EMPTY = IntVersion.builder().intValue(Integer.MIN_VALUE).build();
        private final Integer intValue;

        public static class IntVersionBuilder implements ObjectBuilder<IntVersion> {

        }

        @Override
        public IntVersion asIntVersion() {
            return this;
        }

        @Override
        @SneakyThrows(IOException.class)
        public byte[] toByteArray() {
            return SERIALIZER.serialize(this).getCopy();
        }

        @SneakyThrows(IOException.class)
        public static IntVersion parse(final byte[] data) {
            return SERIALIZER.deserialize(data);
        }
    }
}
