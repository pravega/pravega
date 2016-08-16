/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.controller.server;


import com.emc.pravega.controller.stream.api.ConsumerService;
import com.emc.pravega.controller.stream.api.ProducerService;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

/**
 * Entrypoint for Server. (Initial version)
 */
public class Server {

    public static ConsumerService.Iface consumerService;

    public static ConsumerService.Processor processor;

    public static void main(String[] args) {
        try {
            consumerService = new ConsumerServiceImpl();
            processor = new ConsumerService.Processor(consumerService);

            final TMultiplexedProcessor processor = new TMultiplexedProcessor();
            processor.registerProcessor("consumerService", new ConsumerService.Processor(new ConsumerServiceImpl()));
            processor.registerProcessor("producerService", new ProducerService.Processor(new ProducerServiceImpl()));

            Runnable simple = new Runnable() {
                public void run() {
                    simple(processor);
                }
            };

            new Thread(simple).start();
        } catch (Exception x) {
            x.printStackTrace(); //TODO: enable logging
        }
    }

    public static void simple(final TMultiplexedProcessor processor) {
        try {
            TServerTransport serverTransport = new TServerSocket(9090);
            TServer server = new TSimpleServer(new Args(serverTransport).processor(processor));

            System.out.println("Starting the simple server...");
            server.serve();
        } catch (Exception e) {
            e.printStackTrace(); //TODO: enable logging
        }
    }
}
