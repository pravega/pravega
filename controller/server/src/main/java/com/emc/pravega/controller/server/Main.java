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
import com.emc.pravega.controller.fault.UniformContainerBalancer;
import com.emc.pravega.controller.server.rpc.RPCServer;
import com.emc.pravega.controller.server.rpc.v1.ControllerServiceImpl;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.host.HostStoreFactory;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.stream.StreamStoreFactory;
import com.emc.pravega.controller.util.Config;
import com.emc.pravega.controller.util.ZKUtils;
import lombok.extern.slf4j.Slf4j;

import static com.emc.pravega.controller.util.Config.HOST_STORE_TYPE;
import static com.emc.pravega.controller.util.Config.STREAM_STORE_TYPE;

/**
 * Entry point of controller server.
 */
@Slf4j
public class Main {

    public static void main(String[] args) {
        //LOAD configuration.
        log.info("Creating the stream store");
        StreamMetadataStore streamStore = StreamStoreFactory.createStore(
                StreamStoreFactory.StoreType.valueOf(STREAM_STORE_TYPE), null);

        log.info("Creating the host store");
        HostControllerStore hostStore = HostStoreFactory.createStore(
                HostStoreFactory.StoreType.valueOf(HOST_STORE_TYPE));

        //Host monitor is not required for a single node local setup.
        if (Config.HOST_MONITOR_ENABLED) {
            //Start the segment Container Monitor.
            log.info("Starting the segment container monitor");
            SegmentContainerMonitor monitor = new SegmentContainerMonitor(hostStore,
                    ZKUtils.CuratorSingleton.CURATOR_INSTANCE.getCuratorClient(), Config.CLUSTER_NAME,
                    new UniformContainerBalancer(), Config.CLUSTER_MIN_REBALANCE_INTERVAL);
            monitor.start();
        }

        //Start the RPC server.
        log.info("Starting RPC server");
        RPCServer.start(new ControllerServiceImpl(streamStore, hostStore));
    }
}
