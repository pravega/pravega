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
package io.pravega.test.integration;

import io.pravega.client.ClientConfig;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.ConnectionPoolImpl;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.delegationtoken.PassingTokenVerifier;
import io.pravega.segmentstore.server.host.handler.IndexAppendProcessor;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.host.stat.AutoScaleMonitor;
import io.pravega.segmentstore.server.host.stat.AutoScalerConfig;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.shared.metrics.MetricRegistryUtils;
import io.pravega.shared.metrics.MetricsConfig;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.StatsProvider;
import io.pravega.test.common.SecurityConfigDefaults;
import io.pravega.test.common.SerializedClassRunner;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.ThreadPooledTestSuite;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.utils.ControllerWrapper;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static io.pravega.shared.MetricsNames.SEGMENT_READ_BYTES;
import static io.pravega.shared.MetricsTags.segmentTags;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
@RunWith(SerializedClassRunner.class)
public class MetricsTest extends ThreadPooledTestSuite {

    private static final String STREAM_NAME = "testMetricsStream" + new Random().nextInt(Integer.MAX_VALUE);
    private static final long TOTAL_NUM_EVENTS = 10;
    private final String scope = "testMetricsScope";
    private final String readerGroupName = "testMetricsReaderGroup";
    private final ScalingPolicy scalingPolicy = ScalingPolicy.fixed(1);
    private final StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(scalingPolicy).build();
    private final String readerName = "reader" + new Random().nextInt(Integer.MAX_VALUE);
    private TestingServer zkTestServer = null;
    private PravegaConnectionListener server = null;
    private ControllerWrapper controllerWrapper = null;
    private Controller controller = null;
    private StatsProvider statsProvider = null;
    private ServiceBuilder serviceBuilder = null;
    private AutoScaleMonitor monitor = null;

    @Override
    protected int getThreadPoolSize() {
        return 5;
    }

    @Before
    public void setup() throws Exception {
        final int controllerPort = TestUtils.getAvailableListenPort();
        final String serviceHost = "localhost";
        final int servicePort = TestUtils.getAvailableListenPort();
        final int containerCount = 4;

        // 1. Start Metrics service
        log.info("Initializing metrics provider ...");

        MetricsConfig metricsConfig = MetricsConfig.builder()
                .with(MetricsConfig.ENABLE_STATISTICS, true)
                .with(MetricsConfig.ENABLE_STATSD_REPORTER, false)
                .build();
        metricsConfig.setDynamicCacheEvictionDuration(Duration.ofSeconds(2));

        MetricsProvider.initialize(metricsConfig);
        statsProvider = MetricsProvider.getMetricsProvider();
        statsProvider.startWithoutExporting();
        log.info("Metrics Stats provider is started");

        // 2. Start ZK
        this.zkTestServer = new TestingServerStarter().start();

        // 3. Start Pravega SegmentStore service.
        serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        monitor = new AutoScaleMonitor(store, AutoScalerConfig.builder().build());
        TableStore tableStore = serviceBuilder.createTableStoreService();

        this.server = new PravegaConnectionListener(false, false, "localhost", servicePort, store, tableStore,
                monitor.getStatsRecorder(), monitor.getTableSegmentStatsRecorder(), new PassingTokenVerifier(),
                null, null, true, this.serviceBuilder.getLowPriorityExecutor(), SecurityConfigDefaults.TLS_PROTOCOL_VERSION,
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));
        this.server.startListening();

        // 4. Start Pravega Controller service
        this.controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), false,
                controllerPort, serviceHost, servicePort, containerCount);
        this.controllerWrapper.awaitRunning();
        this.controller = controllerWrapper.getController();
    }

    @After
    public void tearDown() throws Exception {
        if (this.statsProvider != null) {
            statsProvider.close();
            statsProvider = null;
            log.info("Metrics statsProvider is now closed.");
        }

        if (this.controllerWrapper != null) {
            this.controllerWrapper.close();
            this.controllerWrapper = null;
        }

        if (this.server != null) {
            this.server.close();
            this.server = null;
        }

        if (this.monitor != null) {
            this.monitor.close();
            this.monitor = null;
        }

        if (this.serviceBuilder != null) {
            this.serviceBuilder.close();
            this.serviceBuilder = null;
        }

        if (this.zkTestServer != null) {
            this.zkTestServer.close();
            this.zkTestServer = null;
        }
    }

    @Test(timeout = 120000)
    public void metricsTimeBasedCacheEvictionTest() throws Exception {
        ClientConfig clientConfig = ClientConfig.builder().build();
        try (ConnectionPool cp = new ConnectionPoolImpl(clientConfig, new SocketConnectionFactoryImpl(clientConfig));
             StreamManager streamManager = new StreamManagerImpl(controller, cp)) {
            boolean createScopeStatus = streamManager.createScope(scope);
            log.info("Create scope status {}", createScopeStatus);

            boolean createStreamStatus = streamManager.createStream(scope, STREAM_NAME, config);
            log.info("Create stream status {}", createStreamStatus);
        }

        try (ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder().build());
             ClientFactoryImpl clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory);
             ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, controller, clientFactory)) {
            @Cleanup
            EventStreamWriter<String> writer1 = clientFactory.createEventWriter(STREAM_NAME,
                    new UTF8StringSerializer(),
                    EventWriterConfig.builder().build());
            String event = "12345";
            long bytesWritten = TOTAL_NUM_EVENTS * (8 + event.length());

            writeEvents(event, writer1);

            String readerGroupName1 = readerGroupName + "1";
            log.info("Creating Reader group : {}", readerGroupName1);

            readerGroupManager.createReaderGroup(readerGroupName1,
                    ReaderGroupConfig
                            .builder()
                            .stream(Stream.of(scope, STREAM_NAME))
                            .automaticCheckpointIntervalMillis(2000)
                            .build());

            EventStreamReader<String> reader1 = clientFactory.createReader(readerName,
                    readerGroupName1,
                    new UTF8StringSerializer(),
                    ReaderConfig.builder().build());

            readAllEvents(reader1);

            final String[] streamTags = segmentTags(scope + "/" + STREAM_NAME + "/0.#epoch.0");
            assertEquals(bytesWritten, (long) MetricRegistryUtils.getCounter(SEGMENT_READ_BYTES, streamTags).count());

            //Wait for cache eviction to happen
            Thread.sleep(5000);

            String readerGroupName2 = readerGroupName + "2";
            log.info("Creating Reader group : {}", readerGroupName2);

            readerGroupManager.createReaderGroup(readerGroupName2,
                    ReaderGroupConfig.builder()
                            .stream(Stream.of(scope, STREAM_NAME))
                            .automaticCheckpointIntervalMillis(2000)
                            .build());

            EventStreamReader<String> reader2 = clientFactory.createReader(readerName,
                    readerGroupName2,
                    new UTF8StringSerializer(),
                    ReaderConfig.builder().build());

            readAllEvents(reader2);

            //Metric is evicted from Cache, after cache eviction duration
            //Count starts from 0, rather than adding up to previously ready bytes, as cache is evicted.
            assertEquals(bytesWritten, (long) MetricRegistryUtils.getCounter(SEGMENT_READ_BYTES, streamTags).count());

            Map<Double, Double> map = new HashMap<>();
            map.put(0.0, 1.0);

            //Seal segment 0, create segment 1
            CompletableFuture<Boolean> scaleStatus = controller.scaleStream(new StreamImpl(scope, STREAM_NAME),
                    Collections.singletonList(0L),
                    map,
                    executorService()).getFuture();
            Assert.assertTrue(scaleStatus.get());

            @Cleanup
            EventStreamWriter<String> writer2 = clientFactory.createEventWriter(STREAM_NAME,
                    new UTF8StringSerializer(),
                    EventWriterConfig.builder().build());

            writeEvents(event, writer2);

            readAllEvents(reader1);

            final String[] streamTags2nd = segmentTags(scope + "/" + STREAM_NAME + "/1.#epoch.1");
            assertEquals(bytesWritten, (long) MetricRegistryUtils.getCounter(SEGMENT_READ_BYTES, streamTags2nd).count());

            readerGroupManager.deleteReaderGroup(readerGroupName1);
            readerGroupManager.deleteReaderGroup(readerGroupName2);

            CompletableFuture<Boolean> sealStreamStatus = controller.sealStream(scope, STREAM_NAME);
            log.info("Sealing stream {}", STREAM_NAME);
            assertTrue(sealStreamStatus.get());

            CompletableFuture<Boolean> deleteStreamStatus = controller.deleteStream(scope, STREAM_NAME);
            log.info("Deleting stream {}", STREAM_NAME);
            assertTrue(deleteStreamStatus.get());

            CompletableFuture<Boolean> deleteScopeStatus = controller.deleteScope(scope);
            log.info("Deleting scope {}", scope);
            assertTrue(deleteScopeStatus.get());
        }

        log.info("Metrics Time based Cache Eviction test succeeds");
    }

    private void writeEvents(String event, EventStreamWriter<String> writer) {
        for (int i = 0; i < TOTAL_NUM_EVENTS; i++) {
            try {
                log.info("Writing event {}", event);
                writer.writeEvent("", event);
            } catch (Throwable e) {
                log.warn("Test exception writing events: {}", e);
                break;
            }
        }
        writer.flush();
    }

    private void readAllEvents(EventStreamReader<String> reader) {
        for (int q = 0; q < TOTAL_NUM_EVENTS;) {
            try {
                String eventRead2 = reader.readNextEvent(SECONDS.toMillis(2)).getEvent();
                if (eventRead2 != null) {
                    q++;
                }
                log.info("Reading event {}", eventRead2);
            } catch (ReinitializationRequiredException e) {
                log.warn("Test Exception while reading from the stream", e);
            }
        }
    }
       
}
