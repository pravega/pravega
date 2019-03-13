/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.selftest;

import io.pravega.common.Exceptions;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.Timer;
import io.pravega.common.concurrent.Futures;
import io.pravega.test.integration.selftest.adapters.StoreAdapter;
import java.util.HashSet;
import java.util.Queue;
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
        return Futures.loop(
                () -> isRunning() && canContinue.get(),
                this::runOneIteration,
                canContinue::set,
                this.executorService);
    }

    @Override
    protected String getLogId() {
        return "TableConsumer";
    }

    //endregion

    //region TableConsumer Implementation

    private CompletableFuture<Boolean> runOneIteration() {
        return this.dataSource
                .fetchRecentUpdates(this.config.getTableConsumerParallelism())
                .thenComposeAsync(this::processUpdateKeys, this.executorService)
                .handle((r, ex) -> {
                    if (ex != null) {
                        ex.printStackTrace();
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

        val byTarget = updatedKeys.stream().collect(Collectors.groupingBy(TableProducerDataSource.UpdatedKey::getTarget));
        val result = new CompletableFuture[byTarget.size()];
        int index = 0;
        for (val g : byTarget.entrySet()) {
            val timer = new Timer();
            val keys = new HashSet<>(g.getValue()).stream().map(uk -> TableUpdate.generateKey(uk.getKeyId())).collect(Collectors.toList());
            result[index++] = this.store
                    .getTableEntries(g.getKey(), keys, this.config.getTimeout())
                    .thenRun(() -> {
                        this.testState.recordTableGet(keys.size());
                        this.testState.recordDuration(ConsumerOperationType.TABLE_GET, timer.getElapsedMillis());
                    });
        }

        return CompletableFuture.allOf(result).thenApply(v -> true);
    }

    //endregion
}
