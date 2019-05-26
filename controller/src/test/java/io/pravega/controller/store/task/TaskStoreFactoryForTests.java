/**
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.task;

import io.pravega.controller.store.client.StoreClient;
import io.pravega.controller.task.TaskData;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
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

        /**
         * Unblocks a critical section
         */
        void unblockCriticalSection();
    }

    /**
     * Extends {@link ZKTaskMetadataStore} with the calls in {@link TaskMetadataStoreExtensions}.
     */
    public static class ZKTaskMetadataStoreForTests extends ZKTaskMetadataStore implements TaskMetadataStoreExtensions {

        private final AtomicReference<CompletableFuture<Void>> latch = new AtomicReference<>(new CompletableFuture<Void>());

        ZKTaskMetadataStoreForTests(CuratorFramework client, ScheduledExecutorService executor) {
            super(client, executor);
            this.latch.get().complete(null);
        }

        @Override
        public void blockCriticalSection() {
            latch.set(new CompletableFuture<>());
        }

        @Override
        public void unblockCriticalSection() {
            latch.get().complete(null);
        }

        @Override
        public CompletableFuture<Void> lock(final Resource resource,
                                            final TaskData taskData,
                                            final String owner,
                                            final String tag,
                                            final String oldOwner,
                                            final String oldTag) {
            CompletableFuture<Void> future = super.lock(resource, taskData, owner, tag, oldOwner, oldTag);
            try {
                this.latch.get().get();
            } catch (Throwable e) {
                throw new CompletionException(e);
            }

            return future;
        }
    }

    /**
     * Extends {@link InMemoryTaskMetadataStore} with the calls in {@link TaskMetadataStoreExtensions}.
     */
    public static class InMemoryTaskMetadataStoreForTests extends InMemoryTaskMetadataStore
            implements TaskMetadataStoreExtensions {

        private final AtomicReference<CompletableFuture<Void>> latch =
                new AtomicReference<>(new CompletableFuture<Void>());

        InMemoryTaskMetadataStoreForTests(ScheduledExecutorService executor) {
            super(executor);
            this.latch.get().complete(null);
        }

        @Override
        public void blockCriticalSection() {
            latch.set(new CompletableFuture<>());
        }

        @Override
        public void unblockCriticalSection() {
            latch.get().complete(null);
        }

        @Override
        public CompletableFuture<Void> lock(final Resource resource,
                                            final TaskData taskData,
                                            final String owner,
                                            final String tag,
                                            final String oldOwner,
                                            final String oldTag) {
            CompletableFuture<Void> future = super.lock(resource, taskData, owner, tag, oldOwner, oldTag);
            try {
                this.latch.get().get();
            } catch (Throwable e) {
                throw new CompletionException(e);
            }

            return future;
        }
    }
}
