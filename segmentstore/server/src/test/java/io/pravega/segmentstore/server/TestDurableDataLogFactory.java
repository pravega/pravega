/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server;

import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.DurableDataLogFactory;

import java.util.function.Consumer;

/**
 * DurableDataLogFactory for TestDurableDataLog objects.
 */
public class TestDurableDataLogFactory implements DurableDataLogFactory {
    private final DurableDataLogFactory wrappedFactory;
    private final Consumer<TestDurableDataLog> durableLogCreated;

    public TestDurableDataLogFactory(DurableDataLogFactory wrappedFactory) {
        this(wrappedFactory, null);
    }

    public TestDurableDataLogFactory(DurableDataLogFactory wrappedFactory, Consumer<TestDurableDataLog> durableLogCreated) {
        this.wrappedFactory = wrappedFactory;
        this.durableLogCreated = durableLogCreated;
    }

    @Override
    public DurableDataLog createDurableDataLog(int containerId) {
        DurableDataLog log = this.wrappedFactory.createDurableDataLog(containerId);
        TestDurableDataLog result = TestDurableDataLog.create(log);
        if (this.durableLogCreated != null) {
            this.durableLogCreated.accept(result);
        }

        return result;
    }

    @Override
    public void initialize() throws DurableDataLogException {
        this.wrappedFactory.initialize();
    }

    @Override
    public void close() {
        this.wrappedFactory.close();
    }
}
