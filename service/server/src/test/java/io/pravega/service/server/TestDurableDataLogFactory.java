/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.service.server;

import io.pravega.service.storage.DurableDataLog;
import io.pravega.service.storage.DurableDataLogException;
import io.pravega.service.storage.DurableDataLogFactory;

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
