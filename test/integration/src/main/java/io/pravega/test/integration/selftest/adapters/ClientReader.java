/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.test.integration.selftest.adapters;

import io.pravega.common.concurrent.CancellationToken;
import io.pravega.test.integration.selftest.Event;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * StoreReader that reads from a Pravega Client.
 */
public class ClientReader implements StoreReader {
    @Override
    public CompletableFuture<Void> readAll(String streamName, Consumer<ReadItem> eventHandler, CancellationToken cancellationToken) {
        return null;
    }

    @Override
    public CompletableFuture<ReadItem> readExact(String streamName, Object address) {
        return null;
    }

    @Override
    public CompletableFuture<Void> readAllStorage(String streamName, Consumer<Event> eventHandler, CancellationToken cancellationToken) {
        throw new UnsupportedOperationException("readAllStorage is not supported on ClientReader.");
    }
}
