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
package io.pravega.segmentstore.server;

import io.pravega.segmentstore.storage.DebugDurableDataLogWrapper;
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
    public DebugDurableDataLogWrapper createDebugLogWrapper(int logId) {
        return this.wrappedFactory.createDebugLogWrapper(logId);
    }

    @Override
    public int getRepairLogId() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getBackupLogId() {
        return Integer.MAX_VALUE - 1;
    }

    @Override
    public void close() {
        this.wrappedFactory.close();
    }
}
