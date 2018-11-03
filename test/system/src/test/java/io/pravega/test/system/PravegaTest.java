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
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.ControllerImplConfig;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.Utils;
import io.pravega.test.system.framework.services.Service;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.MarathonException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;

@Ignore
@Slf4j
@RunWith(SystemTestRunner.class)
public class PravegaTest extends AbstractSystemTest {

    private final static String STREAM_NAME = "testStreamSampleY";
    private final static String STREAM_SCOPE = "testScopeSampleY";
    private final static String READER_GROUP = "ExampleReaderGroupY";
    private final static int NUM_EVENTS = 100;

    @Rule
    public Timeout globalTimeout = Timeout.seconds(12 * 60);

    private final ScalingPolicy scalingPolicy = ScalingPolicy.fixed(4);
    private final StreamConfiguration config = StreamConfiguration.builder()
                                                                  .scope(STREAM_SCOPE)
                                                                  .streamName(STREAM_NAME)
                                                                  .scalingPolicy(scalingPolicy)
                                                                  .build();

    /**
     * This is used to setup the various services required by the system test framework.
     *
     * @throws InterruptedException If interrupted
     * @throws MarathonException    when error in setup
     * @throws URISyntaxException   If URI is invalid
     */
    @Environment
    public static void initialize() throws MarathonException {
        URI zkUri = startZookeeperInstance();
        startBookkeeperInstances(zkUri);
        URI controllerUri = ensureControllerRunning(zkUri);
        ensureSegmentStoreRunning(zkUri, controllerUri);
    }

    @BeforeClass
    public static void beforeClass() {
        // This is the placeholder to perform any operation on the services before executing the system tests
    }

    /**
     * Invoke the createStream method, ensure we are able to create stream.
     *
     * @throws InterruptedException if interrupted
     * @throws URISyntaxException   If URI is invalid
     * @throws ExecutionException   if error in create stream
     */
    @Before
    public void createStream() throws InterruptedException, ExecutionException {

        Service conService = Utils.createPravegaControllerService(null);

        List<URI> ctlURIs = conService.getServiceDetails();
        URI controllerUri = ctlURIs.get(0);

        log.info("Invoking create stream with Controller URI: {}", controllerUri);
        @Cleanup
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder().build());
        @Cleanup
        ControllerImpl controller = new ControllerImpl(ControllerImplConfig.builder()
                                    .clientConfig(ClientConfig.builder().controllerURI(controllerUri).build())
                                    .build(), connectionFactory.getInternalExecutor());

        assertTrue(controller.createScope(STREAM_SCOPE).get());
        assertTrue(controller.createStream(config).get());
    }

    /**
     * Invoke the simpleTest, ensure we are able to produce  events.
     * The test fails incase of exceptions while writing to the stream.
     *
     * @throws InterruptedException if interrupted
     * @throws URISyntaxException   If URI is invalid
     */
    @Test
    public void simpleTest() throws InterruptedException {

        Service conService = Utils.createPravegaControllerService(null);
        List<URI> ctlURIs = conService.getServiceDetails();
        URI controllerUri = ctlURIs.get(0);

        @Cleanup
        ClientFactory clientFactory = ClientFactory.withScope(STREAM_SCOPE, ClientConfig.builder().controllerURI(controllerUri).build());
        log.info("Invoking Writer test with Controller URI: {}", controllerUri);
        @Cleanup
        EventStreamWriter<Serializable> writer = clientFactory.createEventWriter(STREAM_NAME,
                new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        for (int i = 0; i < NUM_EVENTS; i++) {
            String event = "Publish " + i + "\n";
            log.debug("Producing event: {} ", event);
            writer.writeEvent("", event);
            writer.flush();
            Thread.sleep(500);
        }
        log.info("Invoking Reader test.");
        ReaderGroupManager groupManager = ReaderGroupManager.withScope(STREAM_SCOPE, ClientConfig.builder().controllerURI(controllerUri).build());
        groupManager.createReaderGroup(READER_GROUP, ReaderGroupConfig.builder().stream(Stream.of(STREAM_SCOPE, STREAM_NAME)).build());
        EventStreamReader<String> reader = clientFactory.createReader(UUID.randomUUID().toString(),
                READER_GROUP,
                new JavaSerializer<>(),
                ReaderConfig.builder().build());
        for (int i = 0; i < NUM_EVENTS; i++) {
            try {
                String event = reader.readNextEvent(6000).getEvent();
                if (event != null) {
                    log.debug("Read event: {} ", event);
                }
            } catch (ReinitializationRequiredException e) {
                log.error("Unexpected request to reinitialize {}", e);
                System.exit(0);
            }
        }
        reader.close();
        groupManager.deleteReaderGroup(READER_GROUP);
    }
}
