/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.demo;

import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.server.host.handler.PravegaConnectionListener;
import com.emc.pravega.service.server.store.ServiceBuilder;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.stream.Producer;
import com.emc.pravega.stream.ProducerConfig;
import com.emc.pravega.stream.Transaction;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.emc.pravega.stream.mock.MockClientFactory;

import java.util.concurrent.CompletableFuture;

import org.apache.curator.test.TestingServer;

import lombok.Cleanup;

public class EndToEndTransactionTest {
    public static void main(String[] args) throws Exception {
        @Cleanup
        TestingServer zkTestServer = new TestingServer();
        ControllerWrapper controller = new ControllerWrapper(zkTestServer.getConnectString());

        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize().get();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, StartLocalService.PORT, store);
        server.startListening();

        MockClientFactory clientFactory = new MockClientFactory(StartLocalService.SCOPE, controller);

        @Cleanup
        Producer<String> producer = clientFactory.createProducer(StartLocalService.STREAM_NAME, new JavaSerializer<>(), new ProducerConfig(null));
        Transaction<String> transaction = producer.beginTransaction(60000);

        for (int i = 0; i < 1; i++) {
            String event = "\n Transactional Publish \n";
            System.err.println("Producing event: " + event);
            transaction.publish("", event);
            transaction.flush();
            Thread.sleep(500);
        }

        Transaction<String> transaction2 = producer.beginTransaction(60000);
        for (int i = 0; i < 1; i++) {
            String event = "\n Transactional Publish \n";
            System.err.println("Producing event: " + event);
            transaction2.publish("", event);
            transaction2.flush();
            Thread.sleep(500);
        }

        CompletableFuture<Object> commit = CompletableFuture.supplyAsync(() -> {
            try {
                transaction.commit();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        });

        CompletableFuture<Object> drop = CompletableFuture.supplyAsync(() -> {
            try {
                transaction2.drop();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        });

        CompletableFuture.allOf(commit, drop).get();

        System.exit(0);
    }
}
