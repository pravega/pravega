/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.system;

import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.byteStream.ByteStreamClient;
import io.pravega.client.byteStream.ByteStreamReader;
import io.pravega.client.byteStream.ByteStreamWriter;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.ControllerImplConfig;
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

import static org.junit.Assert.assertTrue;

@Slf4j
@RunWith(SystemTestRunner.class)
public class ByteClientTest extends AbstractSystemTest {

    private static final String STREAM = "testByteClientStream";
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
    private final StreamConfiguration config = StreamConfiguration.builder().scope(SCOPE)
            .streamName(STREAM)
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
        streamManager = StreamManager.create(controllerURI);
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
     * This test verifies the correctness of basic read/write functionality of {@link ByteStreamClient}.
     */
    @Test
    public void byteClientTest() throws IOException {
        @Cleanup
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder().build());
        ControllerImpl controller = new ControllerImpl(ControllerImplConfig.builder()
                .clientConfig(ClientConfig.builder()
                        .controllerURI(controllerURI).build()).build(),
                connectionFactory.getInternalExecutor());
        @Cleanup
        ClientFactory clientFactory = new ClientFactoryImpl(SCOPE, controller);
        log.info("Invoking byteClientTest test with Controller URI: {}", controllerURI);

        ByteStreamClient byteStreamClient = clientFactory.createByteStreamClient();
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

    // End utils region
}
