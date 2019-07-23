/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.datastore;

import io.pravega.common.util.BufferView;

public interface DataStore extends AutoCloseable {

    int getBlockAlignment();

    int getMaxEntryLength();

    @Override
    void close();

    int insert(BufferView data);

    int replace(int address, BufferView data);

    int getAppendableLength(int currentLength);

    int append(int address, int expectedLength, BufferView data);

    boolean delete(int address);

    BufferView get(int address);

    StoreSnapshot getSnapshot();

}
