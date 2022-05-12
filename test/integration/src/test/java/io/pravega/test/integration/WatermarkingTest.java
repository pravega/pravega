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
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.state.Revision;
import io.pravega.client.state.RevisionedStreamClient;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.TimeWindow;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.client.watermark.WatermarkSerializer;
import io.pravega.common.concurrent.Futures;
import io.pravega.shared.NameUtils;
import io.pravega.shared.watermarks.Watermark;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Collection of tests to validate controller bootstrap sequence.
 */
@Slf4j
public class WatermarkingTest extends ThreadPooledTestSuite {

    @ClassRule
    public static final PravegaResource PRAVEGA = new PravegaResource();
    private final AtomicLong timer = new AtomicLong();

    @Override
    protected int getThreadPoolSize() {
        return 5;
    }

    @Test(timeout = 120000)
    public void watermarkTest() throws Exception {
        Controller controller = PRAVEGA.getLocalController();
        String scope = "scope";
        String stream = "watermarkTest";
        StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(5)).build();
        ClientConfig clientConfig = ClientConfig.builder().controllerURI(PRAVEGA.getControllerURI()).build();
        @Cleanup
        StreamManager streamManager = StreamManager.create(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, config);

        Stream streamObj = Stream.of(scope, stream);

        // create 2 writers

        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
        JavaSerializer<Long> javaSerializer = new JavaSerializer<>();
        @Cleanup
        EventStreamWriter<Long> writer1 = clientFactory.createEventWriter(stream, javaSerializer,
                EventWriterConfig.builder().build());
        @Cleanup
        EventStreamWriter<Long> writer2 = clientFactory.createEventWriter(stream, javaSerializer,
                EventWriterConfig.builder().build());

        AtomicBoolean stopFlag = new AtomicBoolean(false);
        // write events
        CompletableFuture<Void> writer1Future = writeEvents(writer1, stopFlag);
        CompletableFuture<Void> writer2Future = writeEvents(writer2, stopFlag);

        // scale the stream several times so that we get complex positions
        scale(controller, streamObj, config);

        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(clientConfig);
        @Cleanup
        ClientFactoryImpl syncClientFactory = new ClientFactoryImpl(scope,
                new ControllerImpl(ControllerImplConfig.builder().clientConfig(clientConfig).build(),
                        connectionFactory.getInternalExecutor()),
                connectionFactory);

        String markStream = NameUtils.getMarkStreamForStream(stream);
        @Cleanup
        RevisionedStreamClient<Watermark> watermarkReader = syncClientFactory.createRevisionedStreamClient(markStream,
                new WatermarkSerializer(),
                SynchronizerConfig.builder().build());

        LinkedBlockingQueue<Watermark> watermarks = new LinkedBlockingQueue<>();
        fetchWatermarks(watermarkReader, watermarks, stopFlag);

        AssertExtensions.assertEventuallyEquals(true, () -> watermarks.size() >= 2, 100000);

        stopFlag.set(true);

        writer1Future.join();
        writer2Future.join();

        // read events from the stream
        @Cleanup
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, controller, syncClientFactory);

        Watermark watermark0 = watermarks.take();
        Watermark watermark1 = watermarks.take();
        assertTrue(watermark0.getLowerTimeBound() <= watermark0.getUpperTimeBound());
        assertTrue(watermark1.getLowerTimeBound() <= watermark1.getUpperTimeBound());
        assertTrue(watermark0.getLowerTimeBound() < watermark1.getLowerTimeBound());

        Map<Segment, Long> positionMap0 = watermark0.getStreamCut().entrySet().stream().collect(
                Collectors.toMap(x -> new Segment(scope, stream, x.getKey().getSegmentId()), Map.Entry::getValue));
        Map<Segment, Long> positionMap1 = watermark1.getStreamCut().entrySet().stream().collect(
                Collectors.toMap(x -> new Segment(scope, stream, x.getKey().getSegmentId()), Map.Entry::getValue));

        StreamCut streamCutFirst = new StreamCutImpl(streamObj, positionMap0);
        StreamCut streamCutSecond = new StreamCutImpl(streamObj, positionMap1);
        Map<Stream, StreamCut> firstMarkStreamCut = Collections.singletonMap(streamObj, streamCutFirst);
        Map<Stream, StreamCut> secondMarkStreamCut = Collections.singletonMap(streamObj, streamCutSecond);

        // read from stream cut of first watermark
        String readerGroup = "watermarkTest-group";
        readerGroupManager.createReaderGroup(readerGroup, ReaderGroupConfig.builder().stream(streamObj)
                .startingStreamCuts(firstMarkStreamCut)
                .endingStreamCuts(secondMarkStreamCut)
                .disableAutomaticCheckpoints()
                .build());

        @Cleanup
        final EventStreamReader<Long> reader = clientFactory.createReader("myreader",
                readerGroup,
                javaSerializer,
                ReaderConfig.builder().build());

        EventRead<Long> event = reader.readNextEvent(10000L);
        TimeWindow currentTimeWindow = reader.getCurrentTimeWindow(streamObj);
        while (event.getEvent() != null && currentTimeWindow.getLowerTimeBound() == null && currentTimeWindow.getUpperTimeBound() == null) {
            event = reader.readNextEvent(10000L);
            currentTimeWindow = reader.getCurrentTimeWindow(streamObj);
        }

        assertNotNull(currentTimeWindow.getUpperTimeBound());

        // read all events and verify that all events are below the bounds
        while (event.getEvent() != null) {
            Long time = event.getEvent();
            log.info("timewindow = {} event = {}", currentTimeWindow, time);
            assertTrue(currentTimeWindow.getLowerTimeBound() == null || time >= currentTimeWindow.getLowerTimeBound());
            assertTrue(currentTimeWindow.getUpperTimeBound() == null || time <= currentTimeWindow.getUpperTimeBound());

            TimeWindow nextTimeWindow = reader.getCurrentTimeWindow(streamObj);
            assertTrue(currentTimeWindow.getLowerTimeBound() == null || nextTimeWindow.getLowerTimeBound() >= currentTimeWindow.getLowerTimeBound());
            assertTrue(currentTimeWindow.getUpperTimeBound() == null || nextTimeWindow.getUpperTimeBound() >= currentTimeWindow.getUpperTimeBound());
            currentTimeWindow = nextTimeWindow;

            event = reader.readNextEvent(10000L);
            if (event.isCheckpoint()) {
                event = reader.readNextEvent(10000L);
            }
        }

        assertNotNull(currentTimeWindow.getLowerTimeBound());
    }


    @Test(timeout = 120000)
    public void recreateStreamWatermarkTest() throws Exception {
        StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(5)).build();
        ClientConfig clientConfig = ClientConfig.builder().controllerURI(PRAVEGA.getControllerURI()).build();

        @Cleanup
        StreamManager streamManager = StreamManager.create(clientConfig);

        // in each iteration create stream, note times and let watermark be generated.
        // then delete stream and move to next iteration and verify that watermarks are generated.
        for (int i = 0; i < 2; i++) {
            String scope = "scope";
            String stream = "recreateStreamWatermarkTest";

            streamManager.createScope(scope);
            streamManager.createStream(scope, stream, config);
            // create writer
            @Cleanup
            EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
            JavaSerializer<Long> javaSerializer = new JavaSerializer<>();
            @Cleanup
            EventStreamWriter<Long> writer = clientFactory.createEventWriter(stream, javaSerializer,
                    EventWriterConfig.builder().build());

            AtomicBoolean stopFlag = new AtomicBoolean(false);
            // write events
            CompletableFuture<Void> writerFuture = writeEvents(writer, stopFlag);

            @Cleanup
            SynchronizerClientFactory syncClientFactory = SynchronizerClientFactory.withScope(scope, clientConfig);

            String markStream = NameUtils.getMarkStreamForStream(stream);
            @Cleanup
            RevisionedStreamClient<Watermark> watermarkReader = syncClientFactory.createRevisionedStreamClient(markStream,
                    new WatermarkSerializer(),
                    SynchronizerConfig.builder().build());

            LinkedBlockingQueue<Watermark> watermarks = new LinkedBlockingQueue<>();
            fetchWatermarks(watermarkReader, watermarks, stopFlag);

            AssertExtensions.assertEventuallyEquals(true, () -> watermarks.size() >= 2, 100000);
            // stop run and seal and delete stream
            stopFlag.set(true);
            writerFuture.join();
            streamManager.sealStream(scope, stream);
            streamManager.deleteStream(scope, stream);
        }
    }

    @Test(timeout = 120000)
    public void watermarkTxnTest() throws Exception {
        Controller controller = PRAVEGA.getLocalController();
        String scope = "scopeTx";
        String stream = "watermarkTxnTest";
        ClientConfig clientConfig = ClientConfig.builder().controllerURI(PRAVEGA.getControllerURI()).build();
        StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(5)).build();

        @Cleanup
        StreamManager streamManager = StreamManager.create(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, config);

        Stream streamObj = Stream.of(scope, stream);

        // create 2 writers

        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
        JavaSerializer<Long> javaSerializer = new JavaSerializer<>();
        @Cleanup
        TransactionalEventStreamWriter<Long> writer1 = clientFactory
                .createTransactionalEventWriter("writer1", stream, new JavaSerializer<>(),
                EventWriterConfig.builder().transactionTimeoutTime(10000).build());
        @Cleanup
        TransactionalEventStreamWriter<Long> writer2 = clientFactory
                .createTransactionalEventWriter("writer2", stream, new JavaSerializer<>(),
                        EventWriterConfig.builder().transactionTimeoutTime(10000).build());

        AtomicBoolean stopFlag = new AtomicBoolean(false);
        // write events
        CompletableFuture<Void> writer1Future = writeTxEvents(writer1, stopFlag);
        CompletableFuture<Void> writer2Future = writeTxEvents(writer2, stopFlag);

        // scale the stream several times so that we get complex positions
        scale(controller, streamObj, config);

        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(clientConfig);
        @Cleanup
        ClientFactoryImpl syncClientFactory = new ClientFactoryImpl(scope,
                new ControllerImpl(ControllerImplConfig.builder().clientConfig(clientConfig).build(),
                        connectionFactory.getInternalExecutor()),
                connectionFactory);

        String markStream = NameUtils.getMarkStreamForStream(stream);
        @Cleanup
        RevisionedStreamClient<Watermark> watermarkReader = syncClientFactory.createRevisionedStreamClient(markStream,
                new WatermarkSerializer(),
                SynchronizerConfig.builder().build());

        LinkedBlockingQueue<Watermark> watermarks = new LinkedBlockingQueue<>();
        fetchWatermarks(watermarkReader, watermarks, stopFlag);

        AssertExtensions.assertEventuallyEquals(true, () -> watermarks.size() >= 2, 100000);

        stopFlag.set(true);

        writer1Future.join();
        writer2Future.join();

        // read events from the stream
        @Cleanup
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, controller, syncClientFactory);

        Watermark watermark0 = watermarks.take();
        Watermark watermark1 = watermarks.take();
        assertTrue(watermark0.getLowerTimeBound() <= watermark0.getUpperTimeBound());
        assertTrue(watermark1.getLowerTimeBound() <= watermark1.getUpperTimeBound());
        assertTrue(watermark0.getLowerTimeBound() < watermark1.getLowerTimeBound());

        Map<Segment, Long> positionMap0 = watermark0.getStreamCut().entrySet().stream().collect(
                Collectors.toMap(x -> new Segment(scope, stream, x.getKey().getSegmentId()), Map.Entry::getValue));
        Map<Segment, Long> positionMap1 = watermark1.getStreamCut().entrySet().stream().collect(
                Collectors.toMap(x -> new Segment(scope, stream, x.getKey().getSegmentId()), Map.Entry::getValue));

        StreamCut streamCutFirst = new StreamCutImpl(streamObj, positionMap0);
        StreamCut streamCutSecond = new StreamCutImpl(streamObj, positionMap1);
        Map<Stream, StreamCut> firstMarkStreamCut = Collections.singletonMap(streamObj, streamCutFirst);
        Map<Stream, StreamCut> secondMarkStreamCut = Collections.singletonMap(streamObj, streamCutSecond);

        // read from stream cut of first watermark
        String readerGroup = "watermarkTxnTest-group";
        readerGroupManager.createReaderGroup(readerGroup, ReaderGroupConfig.builder().stream(streamObj)
                .startingStreamCuts(firstMarkStreamCut)
                .endingStreamCuts(secondMarkStreamCut).disableAutomaticCheckpoints()
                .build());

        @Cleanup
        final EventStreamReader<Long> reader = clientFactory.createReader("myreaderTx",
                readerGroup,
                javaSerializer,
                ReaderConfig.builder().build());

        EventRead<Long> event = reader.readNextEvent(10000L);
        TimeWindow currentTimeWindow = reader.getCurrentTimeWindow(streamObj);
        while (event.getEvent() != null && currentTimeWindow.getLowerTimeBound() == null && currentTimeWindow.getUpperTimeBound() == null) {
            event = reader.readNextEvent(10000L);
            currentTimeWindow = reader.getCurrentTimeWindow(streamObj);
        }

        assertNotNull(currentTimeWindow.getUpperTimeBound());

        // read all events and verify that all events are below the bounds
        while (event.getEvent() != null) {
            Long time = event.getEvent();
            log.info("timewindow = {} event = {}", currentTimeWindow, time);
            assertTrue(currentTimeWindow.getLowerTimeBound() == null || time >= currentTimeWindow.getLowerTimeBound());
            assertTrue(currentTimeWindow.getUpperTimeBound() == null || time <= currentTimeWindow.getUpperTimeBound());

            TimeWindow nextTimeWindow = reader.getCurrentTimeWindow(streamObj);
            assertTrue(currentTimeWindow.getLowerTimeBound() == null || nextTimeWindow.getLowerTimeBound() >= currentTimeWindow.getLowerTimeBound());
            assertTrue(currentTimeWindow.getUpperTimeBound() == null || nextTimeWindow.getUpperTimeBound() >= currentTimeWindow.getUpperTimeBound());
            currentTimeWindow = nextTimeWindow;

            event = reader.readNextEvent(10000L);
            if (event.isCheckpoint()) {
                event = reader.readNextEvent(10000L);
            }
        }

        assertNotNull(currentTimeWindow.getLowerTimeBound());
    }

    @Test(timeout = 60000)
    public void progressingWatermarkWithWriterTimeouts() throws Exception {
        String scope = "Timeout";
        String streamName = "progressingWatermarkWithWriterTimeouts";
        int numSegments = 1;

        ClientConfig clientConfig = ClientConfig.builder().controllerURI(PRAVEGA.getControllerURI()).build();

        @Cleanup
        StreamManager streamManager = StreamManager.create(clientConfig);
        assertNotNull(streamManager);

        streamManager.createScope(scope);

        streamManager.createStream(scope, streamName, StreamConfiguration.builder()
                                                                         .scalingPolicy(ScalingPolicy.fixed(numSegments))
                                                                         .build());
        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
        @Cleanup
        SynchronizerClientFactory syncClientFactory = SynchronizerClientFactory.withScope(scope, clientConfig);

        String markStream = NameUtils.getMarkStreamForStream(streamName);
        @Cleanup
        RevisionedStreamClient<Watermark> watermarkReader = syncClientFactory.createRevisionedStreamClient(markStream,
                new WatermarkSerializer(),
                SynchronizerConfig.builder().build());

        LinkedBlockingQueue<Watermark> watermarks = new LinkedBlockingQueue<>();
        AtomicBoolean stopFlag = new AtomicBoolean(false);
        fetchWatermarks(watermarkReader, watermarks, stopFlag);

        // create two writers and write two sevent and call note time for each writer.
        @Cleanup
        EventStreamWriter<String> writer1 = clientFactory.createEventWriter(streamName,
                new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        writer1.writeEvent("1").get();
        writer1.noteTime(100L);

        @Cleanup
        EventStreamWriter<String> writer2 = clientFactory.createEventWriter(streamName,
                new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        writer2.writeEvent("2").get();
        writer2.noteTime(102L);

        // writer0 should timeout. writer1 and writer2 should result in two more watermarks with following times:
        // 1: 100L-101L 2: 101-101
        // then first writer should timeout and be discarded. But second writer should continue to be active as its time
        // is higher than first watermark. This should result in a second watermark to be emitted.
        AssertExtensions.assertEventuallyEquals(true, () -> watermarks.size() == 2, 100000);
        Watermark watermark1 = watermarks.poll();
        Watermark watermark2 = watermarks.poll();
        assertEquals(100L, watermark1.getLowerTimeBound());
        assertEquals(102L, watermark1.getUpperTimeBound());

        assertEquals(102L, watermark2.getLowerTimeBound());
        assertEquals(102L, watermark2.getUpperTimeBound());

        // stream cut should be same
        assertTrue(watermark2.getStreamCut().entrySet().stream().allMatch(x -> watermark1.getStreamCut().get(x.getKey()).equals(x.getValue())));

        // bring back writer1 and post an event with note time smaller than current watermark
        writer1.writeEvent("3").get();
        writer1.noteTime(101L);

        // no watermark should be emitted.
        Watermark nullMark = watermarks.poll(10, TimeUnit.SECONDS);
        assertNull(nullMark);
    }
    
    @Test(timeout = 60000)
    public void progressingWatermarkWithTimestampAggregationTimeout() throws Exception {
        String scope = "Timeout"; 
        String streamName = "progressingWatermarkWithTimestampAggregationTimeout";
        int numSegments = 1;

        ClientConfig clientConfig = ClientConfig.builder().controllerURI(PRAVEGA.getControllerURI()).build();

        @Cleanup
        StreamManager streamManager = StreamManager.create(clientConfig);
        assertNotNull(streamManager);

        streamManager.createScope(scope);

        streamManager.createStream(scope, streamName, StreamConfiguration.builder()
                .timestampAggregationTimeout(100L)
                .scalingPolicy(ScalingPolicy.fixed(numSegments))
                .build());
        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
        @Cleanup
        SynchronizerClientFactory syncClientFactory = SynchronizerClientFactory.withScope(scope, clientConfig);

        String markStream = NameUtils.getMarkStreamForStream(streamName);
        @Cleanup
        RevisionedStreamClient<Watermark> watermarkReader = syncClientFactory.createRevisionedStreamClient(markStream,
                new WatermarkSerializer(),
                SynchronizerConfig.builder().build());

        LinkedBlockingQueue<Watermark> watermarks = new LinkedBlockingQueue<>();
        AtomicBoolean stopFlag = new AtomicBoolean(false);
        fetchWatermarks(watermarkReader, watermarks, stopFlag);

        // create two writers and write two sevent and call note time for each writer.
        @Cleanup
        EventStreamWriter<String> writer0 = clientFactory.createEventWriter(streamName,
                new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        writer0.writeEvent("0").get();
        writer0.noteTime(50L);

        @Cleanup
        EventStreamWriter<String> writer1 = clientFactory.createEventWriter(streamName,
                new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        writer1.writeEvent("1").get();
        writer1.noteTime(80L);

        @Cleanup
        EventStreamWriter<String> writer2 = clientFactory.createEventWriter(streamName,
                new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        writer2.writeEvent("2").get();
        writer2.noteTime(150L);

        // writer0, writer1 and writer2 should result in the first watermark with following time:
        // 1: 50L-102L
        // then writer0 should time out and be discarded. The remaining two writers should continue to be active
        // which should result in the second watermark with the time:
        // 2: 100L-102L
        // then writer1 should timeout and be discarded. But writer2 should continue to be active as its time
        // is higher than first watermark. This should result in a third watermark to be emitted.
        AssertExtensions.assertEventuallyEquals(true, () -> watermarks.size() == 2, 100000);

        Watermark watermark1 = watermarks.poll();
        Watermark watermark2 = watermarks.poll();

        assertEquals(50L, watermark1.getLowerTimeBound());
        assertEquals(150L, watermark1.getUpperTimeBound());

        assertEquals(80L, watermark2.getLowerTimeBound());
        assertEquals(150L, watermark2.getUpperTimeBound());

        AssertExtensions.assertEventuallyEquals(true, () -> {
                Watermark w = watermarks.poll();
                boolean flag = w != null && w.getLowerTimeBound() == 150 && w.getUpperTimeBound() == 150;
                return flag;
            }, 100000);

        // stream cut should be same
        assertTrue(watermark2.getStreamCut().entrySet().stream().allMatch(x -> watermark1.getStreamCut().get(x.getKey()).equals(x.getValue())));

        // bring back writer1 and post an event with note time smaller than current watermark
        writer1.writeEvent("3").get();
        writer1.noteTime(90L);

        // no watermark should be emitted.
        AssertExtensions.assertEventuallyEquals(null, () -> watermarks.poll(), 100000);
    }

    private void scale(Controller controller, Stream streamObj, StreamConfiguration configuration) {
        // perform several scales
        int numOfSegments = configuration.getScalingPolicy().getMinNumSegments();
        double delta = 1.0 / numOfSegments;
        for (long segmentNumber = 0; segmentNumber < numOfSegments - 1; segmentNumber++) {
            double rangeLow = segmentNumber * delta;
            double rangeHigh = (segmentNumber + 1) * delta;
            double rangeMid = (rangeHigh + rangeLow) / 2;

            Map<Double, Double> map = new HashMap<>();
            map.put(rangeLow, rangeMid);
            map.put(rangeMid, rangeHigh);
            controller.scaleStream(streamObj, Collections.singletonList(segmentNumber), map, executorService()).getFuture().join();
        }
    }

    private CompletableFuture<Void> writeEvents(EventStreamWriter<Long> writer, AtomicBoolean stopFlag) {
        AtomicInteger count = new AtomicInteger(0);
        AtomicLong currentTime = new AtomicLong();
        return Futures.loop(() -> !stopFlag.get(), () -> Futures.delayedFuture(() -> {
            currentTime.set(timer.incrementAndGet());
            return writer.writeEvent(count.toString(), currentTime.get())
                    .thenAccept(v -> {
                        if (count.incrementAndGet() % 3 == 0) {
                            writer.noteTime(currentTime.get());
                        }
                    });
        }, 1000L, executorService()), executorService());
    }

    private CompletableFuture<Void> writeTxEvents(TransactionalEventStreamWriter<Long> writer, AtomicBoolean stopFlag) {
        AtomicInteger count = new AtomicInteger(0);
        return Futures.loop(() -> !stopFlag.get(), () -> Futures.delayedFuture(() -> {
            AtomicLong currentTime = new AtomicLong();
            Transaction<Long> txn = writer.beginTxn();
            return CompletableFuture.runAsync(() -> {
                try {
                    for (int i = 0; i < 10; i++) {
                        currentTime.set(timer.incrementAndGet());
                        txn.writeEvent(count.toString(), currentTime.get());
                    }
                    txn.commit(currentTime.get());
                } catch (TxnFailedException e) {
                    throw new CompletionException(e);
                }
            });
        }, 1000L, executorService()), executorService());
    }

    private void fetchWatermarks(RevisionedStreamClient<Watermark> watermarkReader, LinkedBlockingQueue<Watermark> watermarks, AtomicBoolean stop) throws Exception {
        AtomicReference<Revision> revision = new AtomicReference<>(watermarkReader.fetchOldestRevision());

        Futures.loop(() -> !stop.get(), () -> Futures.delayedTask(() -> {
            Iterator<Map.Entry<Revision, Watermark>> marks = watermarkReader.readFrom(revision.get());
            if (marks.hasNext()) {
                Map.Entry<Revision, Watermark> next = marks.next();
                watermarks.add(next.getValue());
                revision.set(next.getKey());
            }
            return null;
        }, Duration.ofSeconds(5), executorService()), executorService());
    }

}
