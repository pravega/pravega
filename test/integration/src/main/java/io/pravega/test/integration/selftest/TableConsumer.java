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
package io.pravega.test.integration.selftest;

import io.pravega.common.Exceptions;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.Timer;
import io.pravega.common.concurrent.Futures;
import io.pravega.test.integration.selftest.adapters.StoreAdapter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.val;

/**
 * Consumer for Tables.
 */
public class TableConsumer extends Actor {
    //region Members
    private static final int MAX_KEY_BATCH_SIZE = 32; // The max number of keys we can request at once.
    private final TableProducerDataSource dataSource;
    private final TestState testState;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the TableConsumer class.
     *
     * @param config          Test Configuration.
     * @param dataSource      The {@link TableProducerDataSource} to use for fetching keys to query.
     * @param testState       The {@link TestState} to report statistics.
     * @param store           A StoreAdapter to execute operations on.
     * @param executorService The Executor Service to use for async tasks.
     */
    TableConsumer(@NonNull TestConfig config, @NonNull TableProducerDataSource dataSource, @NonNull TestState testState,
                  @NonNull StoreAdapter store, @NonNull ScheduledExecutorService executorService) {
        super(config, store, executorService);
        this.dataSource = dataSource;
        this.testState = testState;
    }

    //endregion

    //region Actor Implementation

    @Override
    protected CompletableFuture<Void> run() {
        val canContinue = new AtomicBoolean(true);
        return Futures
                .loop(
                        () -> isRunning() && canContinue.get(),
                        this::runOneIteration,
                        canContinue::set,
                        this.executorService)
                .thenComposeAsync(ignored -> invokeIterators(), this.executorService);
    }

    @Override
    protected String getLogId() {
        return "TableConsumer";
    }

    //endregion

    //region TableConsumer Implementation

    private CompletableFuture<Boolean> runOneIteration() {
        return this.dataSource
                .fetchRecentUpdates(Integer.MAX_VALUE)
                .thenComposeAsync(this::processUpdateKeys, this.executorService)
                .handle((r, ex) -> {
                    if (ex != null) {
                        ex = Exceptions.unwrap(ex);
                        if (ex instanceof ObjectClosedException) {
                            // End of the test.
                            return false;
                        } else {
                            // Something else. Upstream code will handle this.
                            throw new CompletionException(ex);
                        }
                    }

                    return r;
                });
    }

    private CompletableFuture<Boolean> processUpdateKeys(Queue<TableProducerDataSource.UpdatedKey> updatedKeys) {
        if (updatedKeys == null) {
            // We are done.
            return CompletableFuture.completedFuture(false);
        }

        // Group keys by Target and dedupe them.
        val byTarget = new HashMap<String, HashSet<UUID>>();
        for (val uk : updatedKeys) {
            byTarget.computeIfAbsent(uk.getTarget(), t -> new HashSet<>()).add(uk.getKeyId());
        }

        val result = new ArrayList<CompletableFuture<Void>>();
        for (val targetSet : byTarget.entrySet()) {
            // Serialize the keys.
            val keys = targetSet.getValue().stream()
                    .map(keyId -> TableUpdate.generateKey(keyId, this.config.getTableKeyLength()))
                    .collect(Collectors.toList());

            // Create a number of consumers that will divide the key space evenly between them and request them (in batch)
            // in parallel.
            int consumerCount = Math.min(keys.size(), this.config.getConsumersPerTable());
            int rangeIndex = 0;
            for (int i = 0; i < consumerCount; i++) {
                int rangeSize = Math.min(MAX_KEY_BATCH_SIZE, (keys.size() - rangeIndex) / (consumerCount - i));
                val timer = new Timer();
                val range = keys.subList(rangeIndex, rangeIndex + rangeSize);
                rangeIndex += rangeSize;
                result.add(this.store
                        .getTableEntries(targetSet.getKey(), range, this.config.getTimeout())
                        .thenRun(() -> {
                            this.testState.recordTableGet(range.size());
                            this.testState.recordDuration(ConsumerOperationType.TABLE_GET, timer.getElapsedMillis());
                        }));
            }
        }

        return Futures.allOf(result).thenApply(v -> true);
    }

    private CompletableFuture<Void> invokeIterators() {
        return Futures.allOf(this.testState
                .getAllStreams().stream()
                .map(this::invokeIterator)
                .collect(Collectors.toList()));
    }

    private CompletableFuture<Void> invokeIterator(TestState.StreamInfo tableInfo) {
        val timer = new Timer();
        return this.store
                .iterateTableEntries(tableInfo.getName(), this.config.getTimeout())
                .thenCompose(entryIterator -> {
                    this.testState.recordDuration(ConsumerOperationType.TABLE_ITERATOR, timer.getElapsedMillis());
                    val canContinue = new AtomicBoolean(true);
                    return Futures.loop(
                            canContinue::get,
                            () -> {
                                val startTime = timer.getElapsedNanos();
                                return entryIterator
                                        .getNext()
                                        .thenAccept(item -> {
                                            this.testState.recordDuration(ConsumerOperationType.TABLE_ITERATOR_STEP,
                                                    (timer.getElapsedNanos() - startTime) / Timer.NANOS_TO_MILLIS);
                                            canContinue.set(item != null);
                                        });
                            },
                            this.executorService);
                });
    }

    //endregion
}
