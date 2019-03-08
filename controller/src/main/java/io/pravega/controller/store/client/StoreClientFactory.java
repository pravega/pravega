/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.client;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.auth.JKSHelper;
import io.pravega.common.auth.ZKTLSUtils;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.curator.RetryPolicy;
import org.apache.curator.RetrySleeper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZookeeperFactory;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Factory method for store clients.
 */
@Slf4j
public class StoreClientFactory {
    private static final int CURATOR_MAX_SLEEP_MS = 1000;

    public static StoreClient createStoreClient(final StoreClientConfig storeClientConfig) {
        switch (storeClientConfig.getStoreType()) {
            case Zookeeper:
                return new ZKStoreClient(createZKClient(storeClientConfig.getZkClientConfig().get()));
            case InMemory:
                return new InMemoryStoreClient();
            default:
                throw new NotImplementedException(storeClientConfig.getStoreType().toString());
        }
    }

    @VisibleForTesting
    public static StoreClient createInMemoryStoreClient() {
        return new InMemoryStoreClient();
    }

    @VisibleForTesting
    public static StoreClient createZKStoreClient(CuratorFramework client) {
        return new ZKStoreClient(client);
    }

    private static CuratorFramework createZKClient(ZKClientConfig zkClientConfig) {
        //Create and initialize the curator client framework.
        CompletableFuture<Void> sessionExpiryFuture = new CompletableFuture<>();
        return createZKClient(zkClientConfig, () -> !sessionExpiryFuture.isDone(), sessionExpiryFuture::complete);
    }

    @VisibleForTesting
    static CuratorFramework createZKClient(ZKClientConfig zkClientConfig, Supplier<Boolean> canRetry, Consumer<Void> expiryHandler) {
        return createZKClient(zkClientConfig, canRetry, expiryHandler, new ZKClientFactory());
    }

    @VisibleForTesting
    public static CuratorFramework createZKClient(ZKClientConfig zkClientConfig, Supplier<Boolean> canRetry, Consumer<Void> expiryHandler, ZKClientFactory zkClientFactory) {
        if (zkClientConfig.isSecureConnectionToZooKeeper()) {
            ZKTLSUtils.setSecureZKClientProperties(zkClientConfig.getTrustStorePath(), JKSHelper.loadPasswordFrom(zkClientConfig.getTrustStorePasswordPath()));
        }

        RetryWrapper retryPolicy = new RetryWrapper(new ExponentialBackoffRetry(zkClientConfig.getInitialSleepInterval(),
                zkClientConfig.getMaxRetries(), CURATOR_MAX_SLEEP_MS), canRetry);

        //Create and initialize the curator client framework.
        CuratorFramework zkClient = CuratorFrameworkFactory.builder()
                .connectString(zkClientConfig.getConnectionString())
                .namespace(zkClientConfig.getNamespace())
                .zookeeperFactory(zkClientFactory)
                .retryPolicy(retryPolicy)
                .sessionTimeoutMs(zkClientConfig.getSessionTimeoutMs())
                .build();
        zkClient.start();

        zkClient.getConnectionStateListenable().addListener((client1, newState) -> {
            if (newState.equals(ConnectionState.LOST)) {
                expiryHandler.accept(null);
            }
        });

        return zkClient;
    }

    @VisibleForTesting
    public static class ZKClientFactory implements ZookeeperFactory {
        private ZooKeeper client;
        private String connectString;
        private int sessionTimeout;
        private boolean canBeReadOnly;
        @VisibleForTesting
        @Getter(AccessLevel.PUBLIC)
        private final AtomicBoolean injectParameterUpdateFailure = new AtomicBoolean(false);

        @Override
        @Synchronized
        public ZooKeeper newZooKeeper(String connectString, int sessionTimeout, Watcher watcher, boolean canBeReadOnly) throws Exception {
            // prevent creating a new client, stick to the same client created earlier
            // this trick prevents curator from re-creating ZK client on session expiry
            if (client == null) {
                Exceptions.checkNotNullOrEmpty(connectString, "connectString");
                Preconditions.checkArgument(sessionTimeout > 0, "sessionTimeout should be a positive integer");
                Preconditions.checkNotNull(watcher, "watcher");
                this.connectString = connectString;
                this.sessionTimeout = sessionTimeout;
                this.canBeReadOnly = canBeReadOnly;
                this.client = new ZooKeeper(connectString, sessionTimeout, watcher, canBeReadOnly);
                return this.client;
            } else {
                try {
                    // Simulates an error related to a change in ZooKeeper client parameters.
                    Preconditions.checkArgument(this.injectParameterUpdateFailure.get(), "Simulating a parameter update in ZooKeeper client.");
                    Preconditions.checkArgument(this.connectString.equals(connectString), "connectString differs");
                    Preconditions.checkArgument(this.sessionTimeout == sessionTimeout, "sessionTimeout differs");
                    Preconditions.checkArgument(this.canBeReadOnly == canBeReadOnly, "canBeReadOnly differs");
                    this.client.register(watcher);
                } catch (IllegalArgumentException e) {
                    log.warn("Input argument for new ZooKeeper client ({}, {}, {}) changed with respect to existing client ({}, {}, {}).",
                        connectString, sessionTimeout, canBeReadOnly, this.connectString, this.sessionTimeout, this.canBeReadOnly, e);
                    closeClient(client);
                }
                return this.client;
            }
        }

        private void closeClient(ZooKeeper client) {
            try {
                client.close();
            } catch (Exception e) {
                // We prevent throwing uncontrolled exceptions here, which may lead Curator to retry indefinitely.
                log.error("Problems attempting to close ZooKeeper client.", e);
            }
        }
    }

    private static class RetryWrapper implements RetryPolicy {
        private final RetryPolicy retryPolicy;
        private final Supplier<Boolean> canRetry;

        public RetryWrapper(RetryPolicy policy, Supplier<Boolean> canRetry) {
            this.retryPolicy = policy;
            this.canRetry = canRetry;
        }

        @Override
        public boolean allowRetry(int retryCount, long elapsedTimeMs, RetrySleeper sleeper) {
            return canRetry.get() && retryPolicy.allowRetry(retryCount, elapsedTimeMs, sleeper);
        }
    }
}
