/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.system;

import com.google.common.primitives.Longs;
import io.pravega.client.ByteStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.byteStream.ByteStreamReader;
import io.pravega.client.byteStream.ByteStreamWriter;
import io.pravega.client.byteStream.impl.ByteStreamClientImpl;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.segment.impl.SegmentTruncatedException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.hash.RandomFactory;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.Utils;
import io.pravega.test.system.framework.services.Service;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.MarathonException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Slf4j
@RunWith(SystemTestRunner.class)
public class ByteClientTest extends AbstractSystemTest {

    private static final String STREAM = "testByteClientStream";
    private static final String STREAM_TRUNCATION = "testByteStreamTruncation";
    private static final String SCOPE = "testByteClientScope" + RandomFactory.getSeed();
    private static final int PARALLELISM = 1;
    private static final int MAX_PAYLOAD_SIZE = 100000000;
    private static final int IO_ITERATIONS = 10;
    private static final ScheduledExecutorService WRITER_EXECUTOR =
            ExecutorServiceHelpers.newScheduledThreadPool(2, "byte-writer");
    private static final ScheduledExecutorService READER_EXECUTOR =
            ExecutorServiceHelpers.newScheduledThreadPool(2, "byte-reader");

    @Rule
    public final Timeout globalTimeout = Timeout.seconds(8 * 60);
    private final ScalingPolicy scalingPolicy = ScalingPolicy.fixed(PARALLELISM);
    private final StreamConfiguration config = StreamConfiguration.builder()
            .scalingPolicy(scalingPolicy).build();
    private URI controllerURI = null;
    private StreamManager streamManager = null;
    private final Random randomFactory = RandomFactory.create();

    /**
     * This is used to setup the services required by the system test framework.
     *
     * @throws MarathonException When error in setup.
     */
    @Environment
    public static void initialize() throws MarathonException {
        URI zkUri = startZookeeperInstance();
        startBookkeeperInstances(zkUri);
        URI controllerUri = ensureControllerRunning(zkUri);
        ensureSegmentStoreRunning(zkUri, controllerUri);
    }

    @Before
    public void setup() {
        Service conService = Utils.createPravegaControllerService(null);
        List<URI> ctlURIs = conService.getServiceDetails();
        controllerURI = ctlURIs.get(0);

        streamManager = StreamManager.create(Utils.buildClientConfig(controllerURI));
        assertTrue("Creating scope", streamManager.createScope(SCOPE));
        assertTrue("Creating stream", streamManager.createStream(SCOPE, STREAM, config));
    }

    @After
    public void tearDown() {
        streamManager.close();
    }

    @AfterClass
    public static void cleanUp() {
        ExecutorServiceHelpers.shutdown(WRITER_EXECUTOR);
        ExecutorServiceHelpers.shutdown(READER_EXECUTOR);
    }

    /**
     * This test verifies the correctness of basic read/write functionality of {@link ByteStreamReader} and {@link ByteStreamWriter}.
     */
    @Test
    public void byteClientTest() throws IOException {
        log.info("byteClientTest:: with security enabled: {}", Utils.AUTH_ENABLED);

        @Cleanup
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(Utils.buildClientConfig(controllerURI));
        ControllerImpl controller = new ControllerImpl(ControllerImplConfig.builder()
                                                                           .clientConfig(Utils.buildClientConfig(controllerURI)).build(),
                connectionFactory.getInternalExecutor());

        log.info("Invoking byteClientTest test with Controller URI: {}", controllerURI);
        @Cleanup
        ByteStreamClientFactory byteStreamClient = new ByteStreamClientImpl(SCOPE, controller, connectionFactory);
        @Cleanup("closeAndSeal")
        ByteStreamWriter writer = byteStreamClient.createByteStreamWriter(STREAM);
        @Cleanup
        ByteStreamReader reader = byteStreamClient.createByteStreamReader(STREAM);

        for (int i = 1; i <= MAX_PAYLOAD_SIZE; i *= 10) {
            final int payloadSize = i;
            // Create the synthetic payload for the write.
            byte[] payload = new byte[payloadSize];
            byte[] readBuffer = new byte[payloadSize];
            randomFactory.nextBytes(payload);
            final int payloadHashCode = Arrays.hashCode(payload);
            log.info("Created synthetic payload of size {} with hashcode {}.", payload.length, payloadHashCode);
            AtomicInteger writerIterations = new AtomicInteger();
            AtomicInteger readerIterations = new AtomicInteger();

            // Write the same synthetic payload multiple times to the Stream.
            CompletableFuture<Void> writerLoop = Futures.loop(() -> writerIterations.get() < IO_ITERATIONS,
                    () -> CompletableFuture.runAsync(() -> {
                        try {
                            log.debug("Writing payload of size: {}. Iteration {}.", payload.length, writerIterations.get());
                            writer.write(payload);
                            if (writerIterations.get() % 2 == 0) {
                                log.debug("Flushing write.");
                                writer.flush();
                            }
                        } catch (IOException e) {
                            throw new CompletionException(e);
                        }
                        writerIterations.incrementAndGet();
                    }, WRITER_EXECUTOR), WRITER_EXECUTOR);

            // Read the written data with a read buffer of the same size than the payload and check that read data is correct.
            CompletableFuture<Void> readerLoop = Futures.loop(() -> readerIterations.get() < IO_ITERATIONS,
                    () -> CompletableFuture.runAsync(() -> {
                        try {
                            int offset = 0;
                            while (offset < payloadSize) {
                                offset += reader.read(readBuffer, offset, readBuffer.length - offset);
                                log.debug("Reading data of size: {}. Iteration {}.", offset, readerIterations.get());
                            }
                            Assert.assertEquals("Read data differs from data written.", payloadHashCode, Arrays.hashCode(readBuffer));
                        } catch (IOException e) {
                            throw new CompletionException(e);
                        }
                        readerIterations.incrementAndGet();
                    }, READER_EXECUTOR), READER_EXECUTOR);

            writerLoop.join();
            readerLoop.join();
        }

        log.debug("Data correctly written/read from Stream: byte client test passed.");
    }

    /**
     * This test verifies the correctness of truncation in a ByteStream using {@link ByteStreamReader} and {@link ByteStreamWriter}.
     */
    @Test
    public void byteClientTruncationTest() throws IOException {
        log.info("byteClientTruncationTest:: with security enabled: {}", Utils.AUTH_ENABLED);

        assertTrue("Creating stream", streamManager.createStream(SCOPE, STREAM_TRUNCATION, config));

        @Cleanup
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(Utils.buildClientConfig(controllerURI));
        ControllerImplConfig controllerConfig = ControllerImplConfig.builder().clientConfig(Utils.buildClientConfig(controllerURI)).build();
        ControllerImpl controller = new ControllerImpl(controllerConfig, connectionFactory.getInternalExecutor());
        log.info("Invoking byteClientTruncationTest test with Controller URI: {}", controllerURI);
        @Cleanup
        ByteStreamClientFactory factory = new ByteStreamClientImpl(SCOPE, controller, connectionFactory);
        @Cleanup("closeAndSeal")
        ByteStreamWriter writer = factory.createByteStreamWriter(STREAM);
        @Cleanup
        ByteStreamReader reader = factory.createByteStreamReader(STREAM);

        // Write events.
        long limit = 10000L;
        LongStream.rangeClosed(1, limit).forEachOrdered(val -> {
            try {
                // write as bytes.
                writer.write(Longs.toByteArray(val));
            } catch (IOException e) {
                log.error("Failed to write to the byte stream ", e);
                fail("IO Error while write to byte stream");
            }
        });

        //read 8 bytes at a time.
        byte[] readBuffer = new byte[Long.BYTES];

        // Number of bytes already fetched by the reader
        assertEquals("The initial offset of reader is zero", 0, reader.getOffset());
        // Number of events that can be read from the byte stream without talking to SegmentStore.
        int eventsToRead = reader.available() / Long.BYTES;
        assertTrue(eventsToRead < limit - 1);

        // Set the truncation offset to a event boundary greater than the prefetched events.
        int truncationOffset = (eventsToRead + 1) * Long.BYTES;

        log.info("Truncation data before offset {}", truncationOffset);
        writer.truncateDataBefore(truncationOffset);

        long lastRead = -1;
        while (reader.getOffset() < limit * Long.BYTES ) {
            try {
                int bytesRead = reader.read(readBuffer); // read 8 bytes
                assertEquals(Long.BYTES, bytesRead);
                long eventRead = Longs.fromByteArray(readBuffer);

                // validate the read data and ensure data before trunation offset cannot be read.
                if (lastRead >= eventRead || (eventRead > eventsToRead && eventRead < eventsToRead + 1) ) {
                    log.error("Invalid event read {} while last event that was read is {}", eventRead, lastRead);
                    fail("Invalid event read");
                }
                lastRead = eventRead;

            } catch (SegmentTruncatedException e) {
                log.info("Segment truncation observed at offset {}", reader.getOffset());
                reader.seekToOffset(truncationOffset);
            }
        }

        log.info("Data correctly written/read from Stream with truncation");
    }

}
