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
import java.io.File;
import java.io.IOException;
import lombok.Synchronized;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZookeeperFactory;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * Factory method for store clients.
 */
public class StoreClientFactory {

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
        if (zkClientConfig.isConnectionToZooKeeperSecure()) {
            System.setProperty("zookeeper.client.secure", "true");
            System.setProperty("zookeeper.clientCnxnSocket", "org.apache.zookeeper.ClientCnxnSocketNetty");
            System.setProperty("zookeeper.ssl.trustStore.location", zkClientConfig.getTrustStorePath());
            System.setProperty("zookeeper.ssl.trustStore.password", loadPasswdFromFile(zkClientConfig.getTrustStorePasswordPath()));
        }
        //Create and initialize the curator client framework.
        CuratorFramework zkClient = CuratorFrameworkFactory.builder()
                .connectString(zkClientConfig.getConnectionString())
                .namespace(zkClientConfig.getNamespace())
                .zookeeperFactory(new ZKClientFactory())
                .retryPolicy(new ExponentialBackoffRetry(zkClientConfig.getInitialSleepInterval(),
                        zkClientConfig.getMaxRetries()))
                .sessionTimeoutMs(zkClientConfig.getSessionTimeoutMs())
                .build();
        zkClient.start();
        return zkClient;
    }

    private static String loadPasswdFromFile(String trustStorePasswordPath) {
        byte[] pwd;
        File passwdFile = new File(trustStorePasswordPath);
        if (passwdFile.length() == 0) {
            return "";
        }
        try {
            pwd = FileUtils.readFileToByteArray(passwdFile);
        } catch (IOException e) {
            return "";
        }
        return new String(pwd).trim();
    }

    private static class ZKClientFactory implements ZookeeperFactory {
        private ZooKeeper client;
        private String connectString;
        private int sessionTimeout;
        private boolean canBeReadOnly;

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
                Preconditions.checkArgument(this.connectString.equals(connectString), "connectString differs");
                Preconditions.checkArgument(this.sessionTimeout == sessionTimeout, "sessionTimeout differs");
                Preconditions.checkArgument(this.canBeReadOnly == canBeReadOnly, "canBeReadOnly differs");
                Preconditions.checkNotNull(watcher, "watcher");
                this.client.register(watcher);
                return this.client;
            }
        }
    }
}
