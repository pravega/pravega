/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.segmentstore.storage.mocks;

import io.pravega.segmentstore.storage.chunklayer.SnapshotInfo;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class InMemorySnapshotInfoStore {

    static private final Map<Integer, SnapshotInfo> DATA = Collections.synchronizedMap(new HashMap<>());

    public static void clear() {
        DATA.clear();
    }

    public CompletableFuture<SnapshotInfo> getSnapshotId(int containerId) {
        return CompletableFuture.completedFuture(DATA.get(containerId));
    }

    public CompletableFuture<Void> setSnapshotId(int containerId, SnapshotInfo checkpoint) {
        DATA.put(containerId, checkpoint);
        return CompletableFuture.completedFuture(null);
    }
}
