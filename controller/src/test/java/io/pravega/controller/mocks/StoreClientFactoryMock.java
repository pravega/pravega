/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.mocks;

import io.pravega.controller.store.client.StoreClientFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class StoreClientFactoryMock extends StoreClientFactory {

    public static class ZKClientFactoryMock extends ZKClientFactory {
        /**
         * This flag instructs this class to close the ZK client after the execution of newZooKeeper method. This is
         * used to simulate the state of the client when a ZK session expiration occurs and any ZK connection parameter
         * has changed (i.e., an IllegalArgumentException is thrown and ZKClientFactory closes the ZK client).
         */
        @Getter
        private final AtomicBoolean closeZKClient = new AtomicBoolean(false);

        public ZooKeeper newZooKeeper(String connectString, int sessionTimeout, Watcher watcher, boolean canBeReadOnly) throws Exception {
            ZooKeeper zooKeeper = super.newZooKeeper(connectString, sessionTimeout, watcher, canBeReadOnly);
            if (closeZKClient.get()) {
                zooKeeper.close();
            }
            return zooKeeper;
        }
    }
}
