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

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.Timer;
import io.pravega.common.concurrent.Futures;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import java.time.Duration;
import java.util.Vector;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Replicated Chunk Storage.
 */
@Slf4j
@Beta
public class ReplicatedChunkStorage extends AsyncBaseChunkStorage {
    final AsyncBaseChunkStorage[] chunkStorages;
    final AtomicReference<int[]> preferredOrder = new AtomicReference<>(null);
    /**
     * Constructor.
     *
     * @param chunkStorages Chunk storages.
     * @param executor  An Executor for async operations.
     */
    public ReplicatedChunkStorage(AsyncBaseChunkStorage[] chunkStorages, Executor executor) {
        super(executor);
        this.chunkStorages = Preconditions.checkNotNull(chunkStorages, "chunkStorages");
        val sequence = new int[chunkStorages.length];
        for (int i = 0; i < chunkStorages.length; i++) {
            sequence[i] = i;
        }
        this.preferredOrder.set(sequence);
    }

    @Override
    public boolean supportsTruncation() {
        return false;
    }

    @Override
    public boolean supportsAppend() {
        return false;
    }

    @Override
    public boolean supportsConcat() {
        return false;
    }

    @Override
    protected CompletableFuture<ChunkInfo> doGetInfoAsync(String chunkName, OperationContext opContext) {
        return executeAny((chunkStorage, innerContext) -> chunkStorage.doGetInfoAsync(chunkName, innerContext), opContext);
    }

    @Override
    protected CompletableFuture<ChunkHandle> doCreateAsync(String chunkName, OperationContext opContext) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException());
    }

    @Override
    protected CompletableFuture<ChunkHandle> doCreateWithContentAsync(String chunkName, int length, InputStream data, OperationContext opContext) {
        try {
            val copy = data.readAllBytes();
            return executeAll((chunkStorage, innerContext) -> chunkStorage.doCreateWithContentAsync(chunkName, length, new ByteArrayInputStream(copy), innerContext), opContext, retValues -> retValues.get(0));
        } catch (IOException e) {
            return CompletableFuture.failedFuture(Exceptions.unwrap(e));
        }
    }

    @Override
    protected CompletableFuture<Boolean> checkExistsAsync(String chunkName, OperationContext opContext) {
        return executeAny((chunkStorage, innerContext) -> chunkStorage.checkExistsAsync(chunkName, innerContext), opContext);
    }

    @Override
    protected CompletableFuture<Void> doDeleteAsync(ChunkHandle handle, OperationContext opContext) {
        return executeAll((chunkStorage, innerContext) -> chunkStorage.doDeleteAsync(handle, innerContext), opContext, retValues -> retValues.get(0));
    }

    @Override
    protected CompletableFuture<ChunkHandle> doOpenReadAsync(String chunkName, OperationContext opContext) {
        return executeAll((chunkStorage, innerContext) -> chunkStorage.doOpenReadAsync(chunkName, innerContext), opContext, retValues -> retValues.get(0));
    }

    @Override
    protected CompletableFuture<ChunkHandle> doOpenWriteAsync(String chunkName, OperationContext opContext) {
        return executeAll((chunkStorage, innerContext) -> chunkStorage.doOpenWriteAsync(chunkName, innerContext), opContext, retValues -> retValues.get(0));
    }

    @Override
    protected CompletableFuture<Integer> doReadAsync(ChunkHandle handle, long fromOffset, int length, byte[] buffer, int bufferOffset, OperationContext opContext) {
        return executeAny((chunkStorage, innerContext) -> chunkStorage.doReadAsync(handle, fromOffset, length, buffer, bufferOffset, innerContext), opContext);
    }

    @Override
    protected CompletableFuture<Integer> doWriteAsync(ChunkHandle handle, long offset, int length, InputStream data, OperationContext opContext) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException());
    }

    @Override
    protected CompletableFuture<Integer> doConcatAsync(ConcatArgument[] chunks, OperationContext opContext) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException());
    }

    @Override
    protected CompletableFuture<Void> doSetReadOnlyAsync(ChunkHandle handle, boolean isReadOnly, OperationContext opContext) {
        return executeAll((chunkStorage, innerContext) -> chunkStorage.doSetReadOnlyAsync(handle, isReadOnly, innerContext), opContext, retValues -> retValues.get(0));
    }

    @Override
    protected CompletableFuture<Boolean> doTruncateAsync(ChunkHandle handle, long offset, OperationContext opContext) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException());
    }

    @Override
    protected CompletableFuture<Long> doGetUsedSpaceAsync(OperationContext opContext) {
        return executeAll((chunkStorage, innerContext) -> chunkStorage.doGetUsedSpaceAsync(innerContext), opContext, retValues -> retValues.get(0));
    }

    private <R> CompletableFuture<R> executeAll(BiFunction<AsyncBaseChunkStorage, OperationContext, CompletableFuture<R>> toExecute, OperationContext operationContext, Function<Vector<CompletableFuture<R>>, CompletableFuture<R>> reducer) {
        val futures = new Vector<CompletableFuture<R>>();
        val  contexts = new OperationContext[chunkStorages.length];
        for (int i = 0; i < chunkStorages.length; i++) {
            val chunkStorage = chunkStorages[i];
            contexts[i] = new OperationContext();
            val future = toExecute.apply(chunkStorage, contexts[i]);
            Preconditions.checkState(future != null);
            futures.add(future);
        }
        Preconditions.checkState(null != futures && futures.size() == chunkStorages.length);
        return Futures.allOf(futures)
                .handleAsync( (v, e)  -> {
                    long max = 0;
                    for (val context: contexts) {
                        max = Math.max(max, context.getInclusiveLatency().toMillis());
                    }
                    operationContext.setInclusiveLatency(Duration.ofMillis(max));
                    if (null != e) {
                        throw new CompletionException(Exceptions.unwrap(e));
                    }
                    return reducer.apply(futures).join();
                }, getExecutor());
    }

    private <R> CompletableFuture<R> executeAny(BiFunction<AsyncBaseChunkStorage, OperationContext, CompletableFuture<R>> toExecute, OperationContext operationContext) {
        val shouldContinue = new AtomicBoolean(true);
        val index = new AtomicInteger(0);
        val retValue = new AtomicReference<CompletableFuture<R>>(null);
        val timer = new Timer();
        return Futures.loop(
                () -> shouldContinue.get() && index.get() < chunkStorages.length,
                () -> {
                    val innerOpContext = new OperationContext();
                    val future = toExecute.apply(chunkStorages[index.get()], innerOpContext);
                    retValue.set(future);
                    return future.handleAsync( (v, e) -> {
                        if (e == null) {
                            shouldContinue.set(false);
                            operationContext.setInclusiveLatency(timer.getElapsed());
                            return v;
                        } else {
                            val ex = Exceptions.unwrap(e);
                            if (isStorageUnavailable(ex)) {
                                // Try with next instance.
                                index.incrementAndGet();
                            } else {
                                throw new CompletionException(ex);
                            }
                        }
                        return null;
                    }, getExecutor());
                },
                r -> CompletableFuture.completedFuture(null),
                getExecutor())
        .thenComposeAsync( v -> retValue.get(), getExecutor());

    }

    private static boolean isStorageUnavailable(Throwable ex) {
        return ex instanceof ChunkNotFoundException || ex instanceof ChunkStorageUnavailableException;
    }
}
