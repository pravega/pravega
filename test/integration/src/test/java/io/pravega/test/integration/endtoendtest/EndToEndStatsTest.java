/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.test.integration.endtoendtest;

import io.pravega.client.ClientFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.host.stat.SegmentStatsRecorder;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.demo.ControllerWrapper;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

@Slf4j
public class EndToEndStatsTest {

    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final String serviceHost = "localhost";
    private final int servicePort = TestUtils.getAvailableListenPort();
    private final int containerCount = 4;
    private TestingServer zkTestServer;
    private PravegaConnectionListener server;
    private ControllerWrapper controllerWrapper;
    private ServiceBuilder serviceBuilder;
    private TestStatsRecorder statsRecorder;

    @Before
    public void setUp() throws Exception {
        zkTestServer = new TestingServerStarter().start();

        serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();

        statsRecorder = new TestStatsRecorder();

        server = new PravegaConnectionListener(false, "localhost", servicePort, store,
                statsRecorder, null, null, null, true);
        server.startListening();

        controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(),
                false,
                controllerPort,
                serviceHost,
                servicePort,
                containerCount);
        controllerWrapper.awaitRunning();
    }

    @After
    public void tearDown() throws Exception {
        controllerWrapper.close();
        server.close();
        serviceBuilder.close();
        zkTestServer.close();
    }

    @Test(timeout = 10000)
    public void testStatsCount() throws Exception {
        StreamConfiguration config = StreamConfiguration.builder()
                .scope("test")
                .streamName("test")
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        Controller controller = controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope("test").get();
        controller.createStream(config).get();
        @Cleanup
        ClientFactory clientFactory = new ClientFactoryImpl("test", controller);

        @Cleanup
        EventStreamWriter<String> test = clientFactory.createEventWriter("test", new JavaSerializer<>(),
                EventWriterConfig.builder().transactionTimeoutTime(10000).build());

        for (int i = 0; i < 10; i++) {
            test.writeEvent("test").get();
        }
        assertEquals(statsRecorder.getSegments().get(StreamSegmentNameUtils.getQualifiedStreamSegmentName("test", "test", 0L)).get(), 10);

        Transaction<String> transaction = test.beginTxn();
        for (int i = 0; i < 10; i++) {
            transaction.writeEvent("0", "txntest1");
        }
        assertEquals(statsRecorder.getSegments().get(StreamSegmentNameUtils.getQualifiedStreamSegmentName("test", "test", 0L)).get(), 10);

        transaction.commit();
        Stream stream = new StreamImpl("test", "test");
        while (!controller.checkTransactionStatus(stream, transaction.getTxnId()).get().equals(Transaction.Status.COMMITTED)) {
            Thread.sleep(100);
        }
        assertEquals(statsRecorder.getSegments().get(StreamSegmentNameUtils.getQualifiedStreamSegmentName("test", "test", 0L)).get(), 20);
    }

    private static class TestStatsRecorder implements SegmentStatsRecorder {
        @Getter
        HashMap<String, AtomicInteger> segments = new HashMap<>();

        @Override
        public void createSegment(String streamSegmentName, byte type, int targetRate) {
            String parent = StreamSegmentNameUtils.getParentStreamSegmentName(streamSegmentName);
            if (parent == null) {
                segments.put(streamSegmentName, new AtomicInteger());
            }
        }

        @Override
        public void sealSegment(String streamSegmentName) {
            segments.remove(streamSegmentName);
        }

        @Override
        public void policyUpdate(String streamSegmentName, byte type, int targetRate) {

        }

        @Override
        public void record(String streamSegmentName, long dataLength, int numOfEvents) {
            segments.computeIfPresent(streamSegmentName, (x, y) -> {
                y.addAndGet(numOfEvents);
                return y;
            });
        }

        @Override
        public void merge(String streamSegmentName, long dataLength, int numOfEvents, long txnCreationTime) {
            segments.computeIfPresent(streamSegmentName, (x, y) -> {
                y.addAndGet(numOfEvents);
                return y;
            });
        }
    }
}