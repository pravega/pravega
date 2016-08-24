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
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

/**
 * Thrift based RPC server implementation. (Initial version)
 */
public class RPCServer {

    public static void start(AdminService.Iface adminService, ConsumerService.Iface consumerService, ProducerService.Iface producerService) {
        try {

            final TMultiplexedProcessor processor = new TMultiplexedProcessor();
            processor.registerProcessor("adminService", new AdminService.Processor(adminService));
            processor.registerProcessor("consumerService", new ConsumerService.Processor(consumerService));
            processor.registerProcessor("producerService", new ProducerService.Processor(producerService));

            Runnable simple = () -> {
                simple(processor);
            };

            new Thread(simple).start();
        } catch (Exception x) {
            x.printStackTrace(); //TODO: enable logging
        }
    }

    private static void simple(final TMultiplexedProcessor processor) {
        try {
            TServerTransport serverTransport = new TServerSocket(9090);
            TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));

            server.serve();
        } catch (Exception e) {
            e.printStackTrace(); //TODO: enable logging
        }
    }
}
