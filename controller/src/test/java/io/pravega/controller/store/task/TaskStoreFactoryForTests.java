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
package io.pravega.controller.store.task;

import io.pravega.controller.store.client.StoreClient;
import io.pravega.controller.task.TaskData;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Class that extends the functionality of {@link TaskMetadataStore} for test purposes.
 */
@Slf4j
public class TaskStoreFactoryForTests {

    public static TaskMetadataStore createStore(StoreClient storeClient, ScheduledExecutorService executor) {
        switch (storeClient.getType()) {
            case Zookeeper:
            case PravegaTable:
                return new ZKTaskMetadataStoreForTests((CuratorFramework) storeClient.getClient(), executor);
            case InMemory:
                return new InMemoryTaskMetadataStoreForTests(executor);
            default:
                throw new NotImplementedException(storeClient.getType().toString());
        }
    }

    public static TaskMetadataStore createInMemoryStore(ScheduledExecutorService executor) {
        return new InMemoryTaskMetadataStoreForTests(executor);
    }

    /**
     * Enables additional functionality for testing in TaskMetadataStore
     */
    interface TaskMetadataStoreExtensions {
        /**
         * Blocks a critical section
         */
        void blockCriticalSection();
    }

    /**
     * Extends {@link ZKTaskMetadataStore} with the calls in {@link TaskMetadataStoreExtensions}.
     */
    public static class ZKTaskMetadataStoreForTests extends ZKTaskMetadataStore implements TaskMetadataStoreExtensions {

        private final AtomicReference<CompletableFuture<Void>> latch = new AtomicReference<>(null);
        private final AtomicBoolean first = new AtomicBoolean();

        ZKTaskMetadataStoreForTests(CuratorFramework client, ScheduledExecutorService executor) {
            super(client, executor);
        }

        @Override
        public void blockCriticalSection() {
            this.latch.set(new CompletableFuture<>());
            this.first.set(true);
        }

        @Override
        public CompletableFuture<Void> lock(final Resource resource,
                                            final TaskData taskData,
                                            final String owner,
                                            final String tag,
                                            final String oldOwner,
                                            final String oldTag) {
            CompletableFuture<Void> future = super.lock(resource, taskData, owner, tag, oldOwner, oldTag);

            CompletableFuture<Void> lf = latch.get();
            if (lf != null && first.getAndSet(false)) {
                log.debug("Waiting on the second thread to request the lock and complete the future");
                lf.join();
            } else if (lf != null) {
                log.debug("I'm the second thread, completing the future");
                lf.complete(null);
                latch.set(null);
            } else {
                log.debug("Latch is null");
            }

            return future;
        }
    }

    /**
     * Extends {@link InMemoryTaskMetadataStore} with the calls in {@link TaskMetadataStoreExtensions}.
     */
    public static class InMemoryTaskMetadataStoreForTests extends InMemoryTaskMetadataStore
            implements TaskMetadataStoreExtensions {

        private final AtomicReference<CompletableFuture<Void>> latch = new AtomicReference<>(null);
        private final AtomicBoolean first = new AtomicBoolean();

        InMemoryTaskMetadataStoreForTests(ScheduledExecutorService executor) {
            super(executor);
        }

        @Override
        public void blockCriticalSection() {
            this.latch.set(new CompletableFuture<>());
            this.first.set(true);
        }

        @Override
        public CompletableFuture<Void> lock(final Resource resource,
                                            final TaskData taskData,
                                            final String owner,
                                            final String tag,
                                            final String oldOwner,
                                            final String oldTag) {
            CompletableFuture<Void> future = super.lock(resource, taskData, owner, tag, oldOwner, oldTag);

            CompletableFuture<Void> lf = latch.get();
            if (lf != null && first.getAndSet(false)) {
                log.debug("Waiting on the second thread to request the lock and complete the future");
                lf.join();
            } else if (lf != null) {
                log.debug("I'm the second thread, completing the future");
                lf.complete(null);
                latch.set(null);
            } else {
                log.debug("Latch is null");
            }

            return future;
        }
    }
}
