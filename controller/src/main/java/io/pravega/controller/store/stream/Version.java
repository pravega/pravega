package io.pravega.controller.store.stream;

import io.pravega.common.ObjectBuilder;
import io.pravega.controller.store.stream.tables.serializers.IntVersionSerializer;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;

import java.io.IOException;

public interface Version {
    IntVersion asIntVersion();

    Version getIdentity();

    byte[] toByteArray();

    @Data
    @Builder
    class IntVersion implements Version {
        public static final IntVersionSerializer SERIALIZER = new IntVersionSerializer();
        public static final IntVersion EMPTY = new IntVersion(Integer.MIN_VALUE);
        private final Integer intValue;

        public static class IntVersionBuilder implements ObjectBuilder<IntVersion> {

        }

        @Override
        public IntVersion asIntVersion() {
            return this;
        }

        @Override
        public String toString() {
            return intValue.toString();
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
