/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.segmentstore.storage.chunklayer;

import lombok.Data;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Stores {@link SnapshotInfo}.
 */
@Data
@RequiredArgsConstructor
public class SnapshotInfoStore {
    private final long containerId;
    @NonNull
    private final Function<SnapshotInfo, CompletableFuture<Void>> setter;
    @NonNull
    private final Supplier<CompletableFuture<SnapshotInfo>> getter;

    /**
     * Read snapshot info.
     *
     * @return A CompletableFuture that, when completed, will contain info about the snapshot.
     * If the operation failed, it will be completed with the appropriate exception.
     */
    public CompletableFuture<SnapshotInfo> readSnapshotInfo() {
        return getter.get();
    }

    /**
     * Save snapshot info.
     *
     * @param snapshotInfo snapshotInfo to set
     * @return A CompletableFuture that, when completed, will indicate that the operation completed.
     * If the operation failed, it will be completed with the appropriate exception.
     */
    public CompletableFuture<Void> writeSnapshotInfo(SnapshotInfo snapshotInfo) {
        return setter.apply(snapshotInfo);
    }
}
