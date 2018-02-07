/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.io.serialization;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Map;

public interface RevisionDataOutput extends DataOutput {
    void length(int length) throws IOException;

    OutputStream getBaseStream();

    default <T> void writeCollection(Collection<T> collection, ElementSerializer<T> elementSerializer) throws IOException {
        if (collection == null) {
            writeInt(0);
            return;
        }

        writeInt(collection.size());
        for (T e : collection) {
            elementSerializer.accept(this, e);
        }
    }

    default <K, V> void writeMap(Map<K, V> map, ElementSerializer<K> keySerializer, ElementSerializer<V> valueSerializer) throws IOException {
        if (map == null) {
            writeInt(0);
            return;
        }

        writeInt(map.size());
        for (Map.Entry<K, V> e : map.entrySet()) {
            keySerializer.accept(this, e.getKey());
            valueSerializer.accept(this, e.getValue());
        }
    }

    @FunctionalInterface
    interface ElementSerializer<T> {
        void accept(RevisionDataOutput dataOutput, T element) throws IOException;
    }

    //region Factory

    static CloseableRevisionDataOutput wrap(OutputStream outputStream) throws IOException {
        if (outputStream instanceof RandomOutputStream) {
            return new RandomRevisionDataOutput(outputStream);
        } else {
            return new NonSeekableRevisionDataOutput(outputStream);
        }
    }

    interface CloseableRevisionDataOutput extends RevisionDataOutput, AutoCloseable {
        @Override
        void close() throws IOException;
    }

    //endregion
}
