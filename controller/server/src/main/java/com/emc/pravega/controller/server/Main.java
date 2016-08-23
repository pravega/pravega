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

import com.emc.pravega.controller.contract.v1.api.Api;
import com.emc.pravega.controller.server.rpc.RPCServer;
import com.emc.pravega.controller.server.rpc.v1.AdminServiceImpl;
import com.emc.pravega.controller.server.rpc.v1.ConsumerServiceImpl;
import com.emc.pravega.controller.server.rpc.v1.ProducerServiceImpl;
import com.emc.pravega.controller.server.v1.Api.AdminImpl;
import com.emc.pravega.controller.server.v1.Api.ConsumerImpl;
import com.emc.pravega.controller.server.v1.Api.ProducerImpl;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.stream.StreamStoreFactory;

/**
 * Entry point of controller server.
 */
public class Main {

    public static void main(String[] args) {
        //1) LOAD configuration.
        // TODO: read store type and construct store configuration based on configuration file
        StreamMetadataStore store = StreamStoreFactory.createStore(StreamStoreFactory.StoreType.InMemory, null);

        //2) initialize implementation objects, with right parameters/configuration.
        //2.1) initialize implementation of Api.Admin
        Api.Admin adminApi = new AdminImpl(store, null);

        //2.2) initialize implementation of Api.Consumer
        Api.Consumer consumerApi = new ConsumerImpl(store, null);

        //2.3) initialize implementation of Api.Producer
        Api.Producer producerApi = new ProducerImpl();

        //3) start the Server implementations.
        //3.1) start RPC server with v1 implementation. Enable other versions if required.
        RPCServer.start(new AdminServiceImpl(adminApi), new ConsumerServiceImpl(consumerApi), new ProducerServiceImpl(producerApi));
    }
}
