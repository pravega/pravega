/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.datastore;

import io.pravega.common.util.BufferView;

/**
 * {@link DataStore} implementation that does nothing. Only to be used for stubbing.
 */
public class NoOpDataStore implements DataStore {
    @Override
    public int getBlockAlignment() {
        return 4096;
    }

    @Override
    public int getMaxEntryLength() {
        return StoreLayout.MAX_ENTRY_LENGTH;
    }

    @Override
    public void close() {
        // Nothing to do.
    }

    @Override
    public int insert(BufferView data) {
        // Nothing to do.
        return 0;
    }

    @Override
    public int replace(int address, BufferView data) {
        // Nothing to do.
        return address;
    }

    @Override
    public int getAppendableLength(int currentLength) {
        return currentLength == 0 ? getBlockAlignment() : currentLength - currentLength % getBlockAlignment();
    }

    @Override
    public int append(int address, int expectedLength, BufferView data) {
        // Nothing to do.
        return Math.min(data.getLength(), getAppendableLength(expectedLength));
    }

    @Override
    public boolean delete(int address) {
        // Nothing to do.
        return true;
    }

    @Override
    public BufferView get(int address) {
        return null;
    }

    @Override
    public StoreSnapshot getSnapshot() {
        return new StoreSnapshot(0, 0, 0, 0, StoreLayout.MAX_TOTAL_SIZE);
    }
}