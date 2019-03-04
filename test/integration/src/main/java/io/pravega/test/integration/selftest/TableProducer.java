/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.selftest;

import io.pravega.test.integration.selftest.adapters.StoreAdapter;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Getter;

public class TableProducer extends Actor {
    private final TableProducerDataSource dataSource;
    @Getter
    private final String logId;

    /**
     * Creates a new instance of the TableProducer class.
     *
     * @param id              Id of this TableProducer.
     * @param config          Test Configuration.
     * @param dataSource      Data Source.
     * @param store           A StoreAdapter to execute operations on.
     * @param executorService The Executor Service to use for async tasks.
     */
    TableProducer(int id, TestConfig config, TableProducerDataSource dataSource, StoreAdapter store, ScheduledExecutorService executorService) {
        super(config, store, executorService);
        this.logId = String.format("TableProducer[%s]", id);
        this.dataSource = dataSource;
    }

    @Override
    protected CompletableFuture<Void> run() {
        // TODO: implement.
        return CompletableFuture.completedFuture(null);
    }
}
