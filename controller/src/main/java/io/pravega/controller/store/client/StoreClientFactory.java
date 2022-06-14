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
package io.pravega.controller.store.client;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.security.JKSHelper;
import io.pravega.common.security.ZKTLSUtils;
import lombok.AccessLevel;
import lombok.Setter;
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
            case PravegaTable:
                return new PravegaTableStoreClient(createZKClient(storeClientConfig.getZkClientConfig().get()));
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
    static CuratorFramework createZKClient(ZKClientConfig zkClientConfig, Supplier<Boolean> canRetry, Consumer<Void> expiryHandler, ZKClientFactory zkClientFactory) {
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

        zkClient.getConnectionStateListenable().addListener((client1, newState) -> {
            if (newState.equals(ConnectionState.LOST)) {
                expiryHandler.accept(null);
            }
        });
        
        zkClient.start();

        return zkClient;
    }

    @VisibleForTesting
    static class ZKClientFactory implements ZookeeperFactory {
        private ZooKeeper client;
        @VisibleForTesting
        @Setter(AccessLevel.PACKAGE)
        private String connectString;
        private int sessionTimeout;
        private boolean canBeReadOnly;

        @Override
        @Synchronized
        public ZooKeeper newZooKeeper(String connectString, int sessionTimeout, Watcher watcher, boolean canBeReadOnly) throws Exception {
            if (client == null) {
                Exceptions.checkNotNullOrEmpty(connectString, "connectString");
                Preconditions.checkArgument(sessionTimeout > 0, "sessionTimeout should be a positive integer");
                this.connectString = connectString;
                this.sessionTimeout = sessionTimeout;
                this.canBeReadOnly = canBeReadOnly;
            }
            // Ensure that a new instance of Zookeeper clients in Curator are always created
            // So it can be resolved to a new IP in the case of a Zookeeper instance restart.
            this.client = new ZooKeeper(this.connectString, this.sessionTimeout, watcher, this.canBeReadOnly);
            return this.client;
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
