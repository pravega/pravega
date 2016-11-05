/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.controller.server;

import com.emc.pravega.controller.fault.SegmentContainerMonitor;
import com.emc.pravega.controller.server.rpc.RPCServer;
import com.emc.pravega.controller.server.rpc.v1.ControllerServiceImpl;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.host.HostStoreFactory;
import com.emc.pravega.controller.store.host.ZKConfig;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.stream.StreamStoreFactory;
import com.emc.pravega.controller.util.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import static com.emc.pravega.controller.util.Config.HOST_STORE_TYPE;
import static com.emc.pravega.controller.util.Config.STREAM_STORE_TYPE;

/**
 * Entry point of controller server.
 */
@Slf4j
public class Main {

    public static void main(String[] args) {

        //1) LOAD configuration.
        log.info("Creating in-memory stream store");
        StreamMetadataStore streamStore = StreamStoreFactory.createStore(
                StreamStoreFactory.StoreType.valueOf(STREAM_STORE_TYPE), null);

        //Create Zookeeper based configuration.
        CuratorFramework zkClient = CuratorFrameworkFactory.newClient(Config.ZK_URL, new ExponentialBackoffRetry(
                Config.ZK_RETRY_SLEEP_MS, Config.ZK_MAX_RETRIES));
        ZKConfig zkConfig = new ZKConfig(zkClient, Config.CLUSTER_NAME);

        log.info("Creating the host store");
        HostControllerStore hostStore = HostStoreFactory.createStore(HostStoreFactory.StoreType.valueOf(HOST_STORE_TYPE),
                zkConfig);

        //Start the segment Container Monitor.
        try (SegmentContainerMonitor monitor = new SegmentContainerMonitor(hostStore, zkClient, Config.CLUSTER_NAME)) {
            monitor.start();
        } catch (Exception e) {
            log.error("Error while starting SegmentContainerMonitor", e);
            throw e;
        }

        //2) Start the Server implementations.
        //2.1) Start RPC server with v1 implementation. Enable other versions if required.
        log.info("Starting RPC server");
        RPCServer.start(new ControllerServiceImpl(streamStore, hostStore));
    }
}
