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

import io.pravega.test.integration.selftest.adapters.StoreAdapter;
import java.util.concurrent.CompletableFuture;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * Base Data Source for a Self Test Producer.
 * @param <T> Type of update to add to Targets.
 */
@RequiredArgsConstructor
abstract class ProducerDataSource<T extends ProducerUpdate> {
    protected static final String LOG_ID = "DataSource";
    @NonNull
    protected final TestConfig config;
    @NonNull
    protected final TestState state;
    @NonNull
    protected final StoreAdapter store;

    /**
     * Generates a new {@link ProducerOperation} that can be executed next.
     * @param producerId Id of the {@link Producer} to generate for.
     * @return A new {@link ProducerOperation}.
     */
    abstract ProducerOperation<T> nextOperation(int producerId);

    /**
     * Creates all Targets (as defined by the actual implementation of this class).
     *
     * @return A CompletableFuture that, when complete, will indicate all targets have been created.
     */
    abstract CompletableFuture<Void> createAll();

    /**
     * Deletes all Targets (as defined by the actual implementation of this class)
     *
     * @return A CompletableFuture that, when complete, will indicate all targets have been deleted.
     */
    abstract CompletableFuture<Void> deleteAll();

    /**
     * Determines if the Target (Stream/Table) with given name is closed for modifications or not.
     */
    boolean isClosed(String name) {
        TestState.StreamInfo si = this.state.getStream(name);
        return si == null || si.isClosed();
    }
}
