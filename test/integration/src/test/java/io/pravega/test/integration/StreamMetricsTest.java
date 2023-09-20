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

import com.google.common.collect.ImmutableMap;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.controller.metrics.StreamMetrics;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateReaderGroupResponse;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.delegationtoken.PassingTokenVerifier;
import io.pravega.segmentstore.server.host.handler.IndexAppendProcessor;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.host.stat.AutoScaleMonitor;
import io.pravega.segmentstore.server.host.stat.AutoScalerConfig;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.shared.MetricsNames;
import io.pravega.shared.NameUtils;
import io.pravega.shared.metrics.MetricsConfig;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.StatsProvider;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.SecurityConfigDefaults;
import io.pravega.test.common.SerializedClassRunner;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.utils.ControllerWrapper;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.val;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static io.pravega.shared.MetricsTags.readerGroupTags;
import static io.pravega.shared.MetricsTags.streamTags;
import static io.pravega.shared.metrics.MetricRegistryUtils.getCounter;
import static io.pravega.shared.metrics.MetricRegistryUtils.getGauge;
import static io.pravega.shared.metrics.MetricRegistryUtils.getTimer;
import static io.pravega.test.common.AssertExtensions.assertEventuallyEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Slf4j
@RunWith(SerializedClassRunner.class)
public class StreamMetricsTest {

    private final ScalingPolicy scalingPolicy = ScalingPolicy.fixed(1);
    private final StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(scalingPolicy).build();
    private TestingServer zkTestServer = null;
    private PravegaConnectionListener server = null;
    private ControllerWrapper controllerWrapper = null;
    private Controller controller = null;
    private StatsProvider statsProvider = null;
    private ServiceBuilder serviceBuilder = null;
    private AutoScaleMonitor monitor = null;
    private int controllerPort;
    private ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(1, "test");

    @Before
    public void setup() throws Exception {
        controllerPort = TestUtils.getAvailableListenPort();
        final String serviceHost = "localhost";
        final int servicePort = TestUtils.getAvailableListenPort();
        final int containerCount = 4;

        // 1. Start Metrics service
        log.info("Initializing metrics provider ...");

        MetricsConfig metricsConfig = MetricsConfig.builder()
                .with(MetricsConfig.ENABLE_STATISTICS, true)
                .with(MetricsConfig.ENABLE_STATSD_REPORTER, false)
                .build();
        metricsConfig.setDynamicCacheEvictionDuration(Duration.ofSeconds(60));

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
                new IndexAppendProcessor(this.serviceBuilder.getLowPriorityExecutor(), store));
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
        ExecutorServiceHelpers.shutdown(executor);
    }

    @Test(timeout = 30000)
    public void testStreamsAndScopesBasicMetricsTests() throws Exception {
        String scopeName = "scopeBasic";
        String streamName = "streamBasic";

        String streamScopedName = NameUtils.getScopedStreamName(scopeName, streamName);
        ReaderGroupConfig rgConfig = ReaderGroupConfig.builder().disableAutomaticCheckpoints()
                .stream(streamScopedName)
                .retentionType(ReaderGroupConfig.StreamDataRetention.AUTOMATIC_RELEASE_AT_LAST_CHECKPOINT)
                .build();
        rgConfig = ReaderGroupConfig.cloneConfig(rgConfig, UUID.randomUUID(), 0L);
        // Here, the system scope and streams are already created.
        assertEquals(1, (long) getCounter(MetricsNames.CREATE_SCOPE).count());
        assertEquals(4, (long) getCounter(MetricsNames.CREATE_STREAM).count());

        controllerWrapper.getControllerService().createScope(scopeName, 0L).get();
        if (!controller.createStream(scopeName, streamName, config).get()) {
            log.error("Stream {} for basic testing already existed, exiting", scopeName + "/" + streamName);
            return;
        }
        // Check that the new scope and stream are accounted in metrics.
        assertEquals(2, (long) getCounter(MetricsNames.CREATE_SCOPE).count());
        assertEquals(5, (long) getCounter(MetricsNames.CREATE_STREAM).count());

        // Update the Stream.
        controllerWrapper.getControllerService().updateStream(scopeName, streamName,
                StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(10)).build(), 0L).get();
        assertEquals(1, (long) getCounter(MetricsNames.globalMetricName(MetricsNames.UPDATE_STREAM)).count());
        assertTrue(getTimerMillis(MetricsNames.CONTROLLER_EVENT_PROCESSOR_UPDATE_STREAM_LATENCY) > 0);

        final String subscriber = "subscriber1";
        CreateReaderGroupResponse createRGStatus = controllerWrapper.getControllerService().createReaderGroup(
                scopeName, subscriber, rgConfig, System.currentTimeMillis(), 0L).get();
        assertEquals(CreateReaderGroupResponse.Status.SUCCESS, createRGStatus.getStatus());
        assertEquals(1, (long) getCounter(MetricsNames.globalMetricName(MetricsNames.CREATE_READER_GROUP)).count());

        final String subscriberScopedName = NameUtils.getScopedReaderGroupName(scopeName, subscriber);
        ImmutableMap<Long, Long> streamCut1 = ImmutableMap.of(0L, 10L);
        controllerWrapper.getControllerService().updateSubscriberStreamCut(scopeName, streamName, subscriberScopedName,
                rgConfig.getReaderGroupId().toString(), 0L, streamCut1, 0L).get();
        assertEquals(1, (long) getCounter(MetricsNames.globalMetricName(MetricsNames.UPDATE_SUBSCRIBER)).count());

        controllerWrapper.getControllerService().updateReaderGroup(scopeName, subscriber, rgConfig, 0L).get();
        assertEquals(1, (long) getCounter(MetricsNames.globalMetricName(MetricsNames.UPDATE_READER_GROUP)).count());

        controllerWrapper.getControllerService().deleteReaderGroup(scopeName, subscriber,
                rgConfig.getReaderGroupId().toString(), 0L).get();
        assertEquals(1, (long) getCounter(MetricsNames.globalMetricName(MetricsNames.DELETE_READER_GROUP)).count());

        // Truncate stream
        controllerWrapper.getControllerService().truncateStream(scopeName, streamName, streamCut1, 0L).get();
        assertTrue(getTimerMillis(MetricsNames.CONTROLLER_EVENT_PROCESSOR_TRUNCATE_STREAM_LATENCY) > 0);

        // Seal the Stream.
        controllerWrapper.getControllerService().sealStream(scopeName, streamName, 0L).get();
        assertEquals(1, (long) getCounter(MetricsNames.SEAL_STREAM).count());
        assertTrue(getTimerMillis(MetricsNames.CONTROLLER_EVENT_PROCESSOR_SEAL_STREAM_LATENCY) > 0);

        // Delete the Stream and Scope and check for the respective metrics.
        controllerWrapper.getControllerService().deleteStream(scopeName, streamName, 0L).get();
        controllerWrapper.getControllerService().deleteScope(scopeName, 0L).get();
        assertEquals(1, (long) getCounter(MetricsNames.DELETE_STREAM).count());
        assertEquals(1, (long) getCounter(MetricsNames.DELETE_SCOPE).count());

        assertTrue(getTimerMillis(MetricsNames.CONTROLLER_EVENT_PROCESSOR_DELETE_STREAM_LATENCY) > 0);

        String failedScope = "failedScope";
        String failedStream = "failedStream";
        String failedRG = "failedRG";
        String[] failedScopeTags = streamTags(failedScope, "");
        String[] failedScopeStreamTags = streamTags(failedScope, failedStream);
        String[] failedRGTags = readerGroupTags(failedRG, failedRG);
        // Exercise the metrics for failed stream and scope creation/deletion.
        StreamMetrics.getInstance().createScopeFailed(failedScope);
        StreamMetrics.getInstance().deleteScopeFailed(failedScope);
        StreamMetrics.getInstance().createStreamFailed(failedScope, failedStream);
        StreamMetrics.getInstance().deleteStreamFailed(failedScope, failedStream);
        StreamMetrics.getInstance().updateStreamFailed(failedScope, failedStream);
        StreamMetrics.getInstance().truncateStreamFailed(failedScope, failedStream);
        StreamMetrics.getInstance().sealStreamFailed(failedScope, failedStream);
        StreamMetrics.getInstance().createReaderGroupFailed(failedRG, failedRG);
        StreamMetrics.getInstance().deleteReaderGroupFailed(failedRG, failedRG);
        StreamMetrics.getInstance().updateReaderGroupFailed(failedRG, failedRG);
        StreamMetrics.getInstance().updateTruncationSCFailed(failedRG, failedRG);
        assertEquals(1, (long) getCounter(MetricsNames.CREATE_SCOPE_FAILED, failedScopeTags).count());
        assertEquals(1, (long) getCounter(MetricsNames.DELETE_SCOPE_FAILED, failedScopeTags).count());
        assertEquals(1, (long) getCounter(MetricsNames.CREATE_STREAM_FAILED, failedScopeStreamTags).count());
        assertEquals(1, (long) getCounter(MetricsNames.DELETE_STREAM_FAILED, failedScopeStreamTags).count());
        assertEquals(1, (long) getCounter(MetricsNames.UPDATE_STREAM_FAILED, failedScopeStreamTags).count());
        assertEquals(1, (long) getCounter(MetricsNames.TRUNCATE_STREAM_FAILED, failedScopeStreamTags).count());
        assertEquals(1, (long) getCounter(MetricsNames.SEAL_STREAM_FAILED, failedScopeStreamTags).count());
        assertEquals(1, (long) getCounter(MetricsNames.CREATE_READER_GROUP_FAILED, failedRGTags).count());
        assertEquals(1, (long) getCounter(MetricsNames.DELETE_READER_GROUP_FAILED, failedRGTags).count());
        assertEquals(1, (long) getCounter(MetricsNames.UPDATE_READER_GROUP_FAILED, failedRGTags).count());
        assertEquals(1, (long) getCounter(MetricsNames.UPDATE_SUBSCRIBER_FAILED, streamTags(failedRG, failedRG)).count());
    }

    private long getTimerMillis(String timerName) {
        val timer = getTimer(timerName);
        return (long) timer.totalTime(TimeUnit.MILLISECONDS);
    }

    @Test(timeout = 30000)
    public void testSegmentSplitMerge() throws Exception {
        String scaleScopeName = "scaleScope";
        String scaleStreamName = "scaleStream";

        controllerWrapper.getControllerService().createScope(scaleScopeName, 0L).get();
        if (!controller.createStream(scaleScopeName, scaleStreamName, config).get()) {
            log.error("Stream {} for scale testing already existed, exiting", scaleScopeName + "/" + scaleStreamName);
            return;
        }
        Stream scaleStream = new StreamImpl(scaleScopeName, scaleStreamName);

        //split to 3 segments
        Map<Double, Double> keyRanges = new HashMap<>();
        keyRanges.put(0.0, 0.33);
        keyRanges.put(0.33, 0.66);
        keyRanges.put(0.66, 1.0);

        if (!controller.scaleStream(scaleStream, Collections.singletonList(0L), keyRanges, executor).getFuture().get()) {
            log.error("Scale stream: splitting segment into three failed, exiting");
            return;
        }

        assertEquals(3, (long) getGauge(MetricsNames.SEGMENTS_COUNT, streamTags(scaleScopeName, scaleStreamName)).value());
        assertEquals(1, (long) getGauge(MetricsNames.SEGMENTS_SPLITS, streamTags(scaleScopeName, scaleStreamName)).value());
        assertEquals(0, (long) getGauge(MetricsNames.SEGMENTS_MERGES, streamTags(scaleScopeName, scaleStreamName)).value());

        //merge back to 2 segments
        keyRanges = new HashMap<>();
        keyRanges.put(0.0, 0.5);
        keyRanges.put(0.5, 1.0);

        if (!controller.scaleStream(scaleStream, Arrays.asList(1L, 2L, 3L), keyRanges, executor).getFuture().get()) {
            log.error("Scale stream: merging segments into two failed, exiting");
            return;
        }

        assertEquals(2, (long) getGauge(MetricsNames.SEGMENTS_COUNT, streamTags(scaleScopeName, scaleStreamName)).value());
        assertEquals(1, (long) getGauge(MetricsNames.SEGMENTS_SPLITS, streamTags(scaleScopeName, scaleStreamName)).value());
        assertEquals(1, (long) getGauge(MetricsNames.SEGMENTS_MERGES, streamTags(scaleScopeName, scaleStreamName)).value());
    }

    @Test(timeout = 30000)
    public void testTransactionMetrics() throws Exception {
        String txScopeName = "scopeTx";
        String txStreamName = "streamTx";

        controllerWrapper.getControllerService().createScope(txScopeName, 0L).get();
        if (!controller.createStream(txScopeName, txStreamName, config).get()) {
            log.error("Stream {} for tx testing already existed, exiting", txScopeName + "/" + txStreamName);
            return;
        }

        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(txScopeName, ClientConfig.builder()
                .controllerURI(URI.create("tcp://localhost:" + controllerPort)).build());
        @Cleanup
        TransactionalEventStreamWriter<String> writer = clientFactory.createTransactionalEventWriter(Stream.of(txScopeName, txStreamName).getStreamName(),
                new JavaSerializer<>(), EventWriterConfig.builder().build());

        Transaction<String> transaction = writer.beginTxn();

        assertEquals(1, (long) getCounter(MetricsNames.CREATE_TRANSACTION, streamTags(txScopeName, txStreamName)).count());

        transaction.writeEvent("Test");
        transaction.flush();
        transaction.commit();

        AssertExtensions.assertEventuallyEquals(true, () -> transaction.checkStatus().equals(Transaction.Status.COMMITTED), 10000);
        AssertExtensions.assertEventuallyEquals(true, () -> getCounter(MetricsNames.COMMIT_TRANSACTION, streamTags(txScopeName, txStreamName)) != null, 10000);
        assertEquals(1, (long) getCounter(MetricsNames.COMMIT_TRANSACTION, streamTags(txScopeName, txStreamName)).count());
        assertTrue(getTimerMillis(MetricsNames.CONTROLLER_EVENT_PROCESSOR_COMMIT_TRANSACTION_LATENCY) > 0);

        Transaction<String> transaction2 = writer.beginTxn();
        transaction2.writeEvent("Test");
        transaction2.abort();

        AssertExtensions.assertEventuallyEquals(true, () -> transaction2.checkStatus().equals(Transaction.Status.ABORTED), 10000);
        AssertExtensions.assertEventuallyEquals(true, () -> getCounter(MetricsNames.ABORT_TRANSACTION, streamTags(txScopeName, txStreamName)) != null, 10000);
        assertEquals(1, (long) getCounter(MetricsNames.ABORT_TRANSACTION, streamTags(txScopeName, txStreamName)).count());
    }

    @Test(timeout = 30000)
    public void testRollingTxnMetrics() throws Exception {
        String scaleRollingTxnScopeName = "scaleRollingTxnScope";
        String scaleRollingTxnStreamName = "scaleRollingTxnStream";

        controllerWrapper.getControllerService().createScope(scaleRollingTxnScopeName, 0L).get();
        if (!controller.createStream(scaleRollingTxnScopeName, scaleRollingTxnStreamName, config).get()) {
            fail("Stream " + scaleRollingTxnScopeName + "/" + scaleRollingTxnStreamName + " for scale testing already existed, test failed");
        }

        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scaleRollingTxnScopeName, ClientConfig.builder()
                .controllerURI(URI.create("tcp://localhost:" + controllerPort)).build());
        @Cleanup
        TransactionalEventStreamWriter<String> writer = clientFactory.createTransactionalEventWriter(Stream.of(scaleRollingTxnScopeName, scaleRollingTxnStreamName).getStreamName(),
                new JavaSerializer<>(), EventWriterConfig.builder().build());
        Transaction<String> transaction = writer.beginTxn();
        transaction.writeEvent("Transactional content");

        //split to 3 segments
        Map<Double, Double> keyRanges = new HashMap<>();
        keyRanges.put(0.0, 0.25);
        keyRanges.put(0.25, 0.75);
        keyRanges.put(0.75, 1.0);

        Stream scaleRollingTxnStream = new StreamImpl(scaleRollingTxnScopeName, scaleRollingTxnStreamName);
        if (!controller.scaleStream(scaleRollingTxnStream, Collections.singletonList(0L), keyRanges, executor).getFuture().get()) {
            fail("Scale stream: splitting segment into three failed, exiting");
        }

        assertEquals(3, (long) getGauge(MetricsNames.SEGMENTS_COUNT, streamTags(scaleRollingTxnScopeName, scaleRollingTxnStreamName)).value());
        assertEquals(1, (long) getGauge(MetricsNames.SEGMENTS_SPLITS, streamTags(scaleRollingTxnScopeName, scaleRollingTxnStreamName)).value());
        assertEquals(0, (long) getGauge(MetricsNames.SEGMENTS_MERGES, streamTags(scaleRollingTxnScopeName, scaleRollingTxnStreamName)).value());

        transaction.flush();
        transaction.commit();

        String message = "Inconsistency found between metadata and metrics";
        assertEventuallyEquals(message, 3L, () -> (long) getGauge(MetricsNames.SEGMENTS_COUNT, streamTags(scaleRollingTxnScopeName, scaleRollingTxnStreamName)).value(), 500, 30000);
        assertEventuallyEquals(message, 2L, () -> (long) getGauge(MetricsNames.SEGMENTS_SPLITS, streamTags(scaleRollingTxnScopeName, scaleRollingTxnStreamName)).value(), 200, 30000);
        assertEventuallyEquals(message, 1L, () -> (long) getGauge(MetricsNames.SEGMENTS_MERGES, streamTags(scaleRollingTxnScopeName, scaleRollingTxnStreamName)).value(), 200, 30000);
    }
}
