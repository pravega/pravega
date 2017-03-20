/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.client;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.NotImplementedException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZookeeperFactory;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/**
 * Factory method for store clients.
 */
public class StoreClientFactory {

    public static StoreClient createStoreClient(final StoreClientConfig storeClientConfig) throws IOException {
        switch (storeClientConfig.getStoreType()) {
            case Zookeeper:
                return new ZKStoreClient(createZKClient(storeClientConfig.getZkClientConfig().get()));
            case InMemory:
                return new InMemoryStoreClient();
            case ECS:
            case S3:
            case HDFS:
            default:
                throw new NotImplementedException();
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

    private static CuratorFramework createZKClient(ZKClientConfig zkClientConfig) throws IOException {
        //Create and initialize the curator client framework.
        CuratorFramework zkClient = CuratorFrameworkFactory.builder()
                .connectString(zkClientConfig.getConnectionString())
                .namespace(zkClientConfig.getNamespace())
                .zookeeperFactory(new ZKClientFactory(zkClientConfig.getConnectionString(), 60000, event -> {
                }, false))
                .retryPolicy(new ExponentialBackoffRetry(zkClientConfig.getInitialSleepInterval(),
                        zkClientConfig.getMaxRetries()))
                .build();
        zkClient.start();
        return zkClient;
    }

    private static class ZKClientFactory implements ZookeeperFactory {
        ZooKeeper client;

        ZKClientFactory(String connectString, int sessionTimeout, Watcher watcher, boolean canBeReadOnly) throws IOException {
            this.client = new ZooKeeper(connectString, sessionTimeout, watcher, canBeReadOnly);
        }

        @Override
        public ZooKeeper newZooKeeper(String connectString, int sessionTimeout, Watcher watcher, boolean canBeReadOnly) throws Exception {
            // prevent creating a new client, stick to the same client created earlier
            // this trick prevents curator from re-creating ZK client on session expiry
            client.register(watcher);
            return client;
        }
    }
}
