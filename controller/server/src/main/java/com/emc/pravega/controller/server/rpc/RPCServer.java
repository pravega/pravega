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

package com.emc.pravega.controller.server.rpc;


import com.emc.pravega.controller.stream.api.v1.AdminService;
import com.emc.pravega.controller.stream.api.v1.ConsumerService;
import com.emc.pravega.controller.stream.api.v1.ProducerService;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * Thrift based RPC server implementation. (Initial version)
 */
@Slf4j
public class RPCServer {

    public static void start(AdminService.Iface adminService, ConsumerService.Iface consumerService, ProducerService.Iface producerService) {
        try {

            final TMultiplexedProcessor processor = new TMultiplexedProcessor();
            processor.registerProcessor("consumerService", new ConsumerService.Processor(consumerService));
            processor.registerProcessor("producerService", new ProducerService.Processor(producerService));

            Runnable server = () -> {
                simpleServer(processor);
            };

            new Thread(server).start();
        } catch (Exception x) {
            log.error("Exception during start of RPC server", x);
        }
    }

    private static void simpleServer(final TMultiplexedProcessor processor) {
        try {
            TServerTransport serverTransport = new TServerSocket(9090);
            TServer server = new TSimpleServer(new Args(serverTransport).processor(processor));

            log.info("Starting the simple server...");
            server.serve();
        } catch (Exception e) {
            log.error("Exception during start of Simple RPC server", e);
        }
    }

    private static void threadedServer(final TMultiplexedProcessor processor) {
        try {
            TNonblockingServerSocket socket = new TNonblockingServerSocket(9090);

            TThreadedSelectorServer.Args config = new TThreadedSelectorServer.Args(socket);
            config.processor(processor)
                    .protocolFactory(new TJSONProtocol.Factory())
                    .workerThreads(10)
                    .selectorThreads(3);

            TServer server = new TThreadedSelectorServer(config);
            log.info("Starting Threaded Selector Server");

        } catch (TTransportException e) {
            log.error("Exception during start of Threaded Selector Server", e);
        }
    }
}
