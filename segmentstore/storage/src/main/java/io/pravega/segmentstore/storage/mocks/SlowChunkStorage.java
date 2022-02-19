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
package io.pravega.segmentstore.storage.mocks;

import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.storage.chunklayer.ChunkHandle;
import io.pravega.segmentstore.storage.chunklayer.ChunkInfo;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorage;
import io.pravega.segmentstore.storage.chunklayer.ConcatArgument;

import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
* Mock for StorageFactory. Contents is destroyed when object is garbage collected.
*
*/
public class SlowChunkStorage implements ChunkStorage {
    final ChunkStorage inner;
    final ScheduledExecutorService executorService;
    final Duration duration;

    /**
     * Creates a new instance of SlowChunkStorage.
     * @param inner inner Storage for this instance
     * @param executorService executorService to be used
     * @param duration duration for that instance
     */
    public SlowChunkStorage(ChunkStorage inner, ScheduledExecutorService executorService, Duration duration) {
        this.inner = inner;
        this.executorService = executorService;
        this.duration = duration;
    }

    @Override
    public boolean supportsTruncation() {
        return inner.supportsTruncation();
    }

    @Override
    public boolean supportsAppend() {
        return inner.supportsAppend();
    }

    @Override
    public boolean supportsConcat() {
        return inner.supportsConcat();
    }

    /**
     * Checks for the existence of the chunk in the chunkStorage.
     * @param chunkName Name of the storage object to check.
     * @return boolean value depending on the chunk's existence
     */
    @Override
    public CompletableFuture<Boolean> exists(String chunkName) {
        return Futures.delayedFuture(duration, executorService).
                thenComposeAsync(v -> inner.exists(chunkName));
    }

    /**
     * Creates a chunk in the chunkStorage.
     * @param chunkName String name of the storage object to create.
     * @return handle to a chunk
     */
    @Override
    public CompletableFuture<ChunkHandle> create(String chunkName) {
        return Futures.delayedFuture(duration, executorService).
                thenComposeAsync(v -> inner.create(chunkName));
    }

    @Override
    public CompletableFuture<ChunkHandle> createWithContent(String chunkName, int length, InputStream data) {
        return Futures.delayedFuture(duration, executorService).
                thenComposeAsync(v -> inner.createWithContent(chunkName, length, data));
    }

    @Override
    public CompletableFuture<Void> delete(ChunkHandle handle) {
        return Futures.delayedFuture(duration, executorService).
                thenComposeAsync(v -> inner.delete(handle));
    }

    @Override
    public CompletableFuture<ChunkHandle> openRead(String chunkName) {
        return Futures.delayedFuture(duration, executorService).
                thenComposeAsync(v -> inner.openRead(chunkName));
    }

    @Override
    public CompletableFuture<ChunkHandle> openWrite(String chunkName) {
        return Futures.delayedFuture(duration, executorService).
                thenComposeAsync(v -> inner.openWrite(chunkName));
    }

    @Override
    public CompletableFuture<ChunkInfo> getInfo(String chunkName) {
        return Futures.delayedFuture(duration, executorService).
                thenComposeAsync(v -> inner.getInfo(chunkName));
    }

    @Override
    public CompletableFuture<Integer> read(ChunkHandle handle, long fromOffset, int length, byte[] buffer, int bufferOffset) {
        return Futures.delayedFuture(duration, executorService).
                thenComposeAsync(v -> inner.read(handle, fromOffset, length, buffer, bufferOffset));
    }

    @Override
    public CompletableFuture<Integer> write(ChunkHandle handle, long offset, int length, InputStream data) {
        return Futures.delayedFuture(duration, executorService).
                thenComposeAsync(v -> inner.write(handle, offset, length, data));
    }

    @Override
    public CompletableFuture<Integer> concat(ConcatArgument[] chunks) {
        return Futures.delayedFuture(duration, executorService).
                thenComposeAsync(v -> inner.concat(chunks));
    }

    @Override
    public CompletableFuture<Boolean> truncate(ChunkHandle handle, long offset) {
        return Futures.delayedFuture(duration, executorService).
                thenComposeAsync(v -> inner.truncate(handle, offset));
    }

    @Override
    public CompletableFuture<Void> setReadOnly(ChunkHandle handle, boolean isReadonly) {
        return Futures.delayedFuture(duration, executorService).
                thenComposeAsync(v -> inner.setReadOnly(handle, isReadonly));
    }

    @Override
    public CompletableFuture<Long> getUsedSpace() {
        return Futures.delayedFuture(duration, executorService).
                thenComposeAsync(v -> inner.getUsedSpace());
    }

    @Override
    public void report() {

    }

    @Override
    public void close() throws Exception {

    }
}
