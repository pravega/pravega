/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.test.integration.endtoendtest;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.IndexAppendProcessor;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.host.stat.SegmentStatsRecorder;
import io.pravega.segmentstore.server.host.stat.TableSegmentStatsRecorder;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.shared.NameUtils;
import io.pravega.test.common.SecurityConfigDefaults;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.utils.ControllerWrapper;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.Cleanup;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.pravega.shared.MetricsNames.SEGMENT_WRITE_BYTES;
import static io.pravega.shared.MetricsNames.SEGMENT_WRITE_EVENTS;
import static io.pravega.shared.MetricsTags.segmentTags;
import static io.pravega.test.common.AssertExtensions.assertEventuallyEquals;

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
        TableStore tableStore = serviceBuilder.createTableStoreService();

        statsRecorder = new TestStatsRecorder();

        server = new PravegaConnectionListener(false, false, "localhost", servicePort, store, tableStore,
                statsRecorder, TableSegmentStatsRecorder.noOp(), null, null, null, true,
                serviceBuilder.getLowPriorityExecutor(), SecurityConfigDefaults.TLS_PROTOCOL_VERSION,
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));
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
    @SuppressWarnings("deprecation")
    public void testStatsCount() throws Exception {
        StreamConfiguration config = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        Controller controller = controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope("test", 0L).get();
        controller.createStream("test", "test", config).get();
        @Cleanup
        EventStreamClientFactory clientFactory = new ClientFactoryImpl("test", controller, ClientConfig.builder().build());

        EventWriterConfig writerConfig = EventWriterConfig.builder().transactionTimeoutTime(10000).build();
        @Cleanup
        EventStreamWriter<String> eventWriter = clientFactory.createEventWriter("test", new JavaSerializer<>(),
                writerConfig);
        @Cleanup
        TransactionalEventStreamWriter<String> txnWriter = clientFactory.createTransactionalEventWriter("test", new JavaSerializer<>(),
                writerConfig);

        String[] tags = segmentTags(NameUtils.getQualifiedStreamSegmentName("test", "test", 0L));

        for (int i = 0; i < 10; i++) {
            eventWriter.writeEvent("test").get();
        }
        assertEventuallyEquals(10, () -> (int) (statsRecorder.getRegistry().counter(SEGMENT_WRITE_EVENTS, tags).count()), 2000);
        assertEventuallyEquals(190, () -> (int) (statsRecorder.getRegistry().counter(SEGMENT_WRITE_BYTES, tags).count()), 100);

        Transaction<String> transaction = txnWriter.beginTxn();
        for (int i = 0; i < 10; i++) {
            transaction.writeEvent("0", "txntest1");
        }
        assertEventuallyEquals(10, () -> (int) (statsRecorder.getRegistry().counter(SEGMENT_WRITE_EVENTS, tags).count()), 2000);
        assertEventuallyEquals(190, () -> (int) (statsRecorder.getRegistry().counter(SEGMENT_WRITE_BYTES, tags).count()), 100);

        transaction.commit();

        assertEventuallyEquals(20, () -> (int) (statsRecorder.getRegistry().counter(SEGMENT_WRITE_EVENTS, tags).count()), 10000);
        assertEventuallyEquals(420, () -> (int) (statsRecorder.getRegistry().counter(SEGMENT_WRITE_BYTES, tags).count()), 100);
    }

    private static class TestStatsRecorder implements SegmentStatsRecorder {

        @Getter
        SimpleMeterRegistry registry = new SimpleMeterRegistry();

        // A placeholder to keep strong references to metric objects, as caching is skipped in the test.
        // Note Micrometer registry holds weak references only, so there is chance metric objects without strong references might be garbage collected.
        private List<Meter> references = Collections.synchronizedList(new ArrayList<Meter>());

        @Override
        public void createSegment(String streamSegmentName, byte type, int targetRate, Duration elapsed) {

        }

        @Override
        public void deleteSegment(String segmentName) {

        }

        @Override
        public void sealSegment(String streamSegmentName) {

        }

        @Override
        public void policyUpdate(String streamSegmentName, byte type, int targetRate) {

        }

        @Override
        public void recordAppend(String streamSegmentName, long dataLength, int numOfEvents, Duration elapsed) {
            if (!NameUtils.isTransactionSegment(streamSegmentName)) {
                Counter eventCounter = registry.counter(SEGMENT_WRITE_EVENTS, segmentTags(streamSegmentName));
                Counter byteCounter = registry.counter(SEGMENT_WRITE_BYTES, segmentTags(streamSegmentName));
                references.add(eventCounter);
                references.add(byteCounter);
                eventCounter.increment(numOfEvents);
                byteCounter.increment(dataLength);
            }
        }

        @Override
        public void merge(String streamSegmentName, long dataLength, int numOfEvents, long txnCreationTime) {
            Counter eventCounter = registry.counter(SEGMENT_WRITE_EVENTS, segmentTags(streamSegmentName));
            Counter byteCounter = registry.counter(SEGMENT_WRITE_BYTES, segmentTags(streamSegmentName));
            references.add(eventCounter);
            references.add(byteCounter);
            eventCounter.increment(numOfEvents);
            byteCounter.increment(dataLength);
        }

        @Override
        public void readComplete(Duration elapsed) {

        }

        @Override
        public void read(String segment, int length) {

        }

        @Override
        public void close() {
        }
    }
}