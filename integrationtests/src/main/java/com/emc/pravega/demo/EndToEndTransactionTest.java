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

import com.emc.pravega.ClientFactory;
import com.emc.pravega.StreamManager;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.server.host.handler.PravegaConnectionListener;
import com.emc.pravega.service.server.store.ServiceBuilder;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.Transaction;
import com.emc.pravega.stream.impl.ClientFactoryImpl;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.emc.pravega.stream.impl.StreamConfigurationImpl;
import com.emc.pravega.stream.impl.StreamManagerImpl;
import com.emc.pravega.stream.impl.netty.ConnectionFactoryImpl;
import lombok.Cleanup;
import org.apache.curator.test.TestingServer;

public class EndToEndTransactionTest {

    static StreamConfigurationImpl config = new StreamConfigurationImpl(StartLocalService.SCOPE, StartLocalService.STREAM_NAME,
            new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 1, 1, 1));

    public static void main(String[] args) throws Exception {
        try {
            @Cleanup
            TestingServer zkTestServer = new TestingServer();
            ControllerWrapper controller = ControllerWrapper.getControllerWrapper(zkTestServer.getConnectString());

            ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
            serviceBuilder.initialize().get();
            StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
            @Cleanup
            PravegaConnectionListener server = new PravegaConnectionListener(false, 12345, store);
            server.startListening();

            ClientFactory clientFactory = new ClientFactoryImpl(StartLocalService.SCOPE, controller, new ConnectionFactoryImpl(false));

            StreamManager streamManager = new StreamManagerImpl(StartLocalService.SCOPE, controller, clientFactory);
            streamManager.createStream(StartLocalService.STREAM_NAME, config);

            @Cleanup
            EventStreamWriter<String> producer = clientFactory.createEventWriter(StartLocalService.STREAM_NAME, new JavaSerializer<>(), new EventWriterConfig(null));
            Transaction<String> transaction = producer.beginTxn(60000);

            for (int i = 0; i < 100; i++) {
                String event = "\n Transactional Publish \n";
                System.err.println("Producing event: " + event);
                transaction.writeEvent("", event);
            }

            Transaction<String> transaction2 = producer.beginTxn(60000);
            for (int i = 0; i < 100; i++) {
                String event = "\n Transactional Publish \n";
                System.err.println("Producing event: " + event);
                transaction2.writeEvent("", event);
            }

            transaction.commit();
            transaction2.abort();
        } catch (Exception e) {
            System.err.println("Failure");
            System.exit(-1);
        }
        System.err.println("Success");

        System.exit(0);
    }
}
