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
import io.pravega.client.admin.StreamManager;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.TruncatedDataException;
import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.ControllerImplConfig;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.common.Exceptions;
import io.pravega.common.hash.RandomFactory;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.Utils;
import io.pravega.test.system.framework.services.Service;
import java.io.Serializable;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.MarathonException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.assertTrue;

@Slf4j
@RunWith(SystemTestRunner.class)
public class RetentionTest {

    private static final String STREAM = "testRetentionStream";
    private static final String SCOPE = "testRetentionScope" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    private static final String READER_GROUP = "testRetentionReaderGroup" + RandomFactory.create().nextInt(Integer.MAX_VALUE);

    @Rule
    public Timeout globalTimeout = Timeout.seconds(8 * 60);

    private final ScalingPolicy scalingPolicy = ScalingPolicy.fixed(2);
    private final RetentionPolicy retentionPolicy = RetentionPolicy.byTime(Duration.ofMinutes(1));
    private final StreamConfiguration config = StreamConfiguration.builder().scope(SCOPE)
            .streamName(STREAM).scalingPolicy(scalingPolicy).retentionPolicy(retentionPolicy).build();
    private URI controllerURI;
    private StreamManager streamManager;



    /**
     * This is used to setup the various services required by the system test framework.
     *
     * @throws MarathonException    when error in setup
     */
    @Environment
    public static void initialize() throws MarathonException {

        //1. check if zk is running, if not start it
        Service zkService = Utils.createZookeeperService();
        if (!zkService.isRunning()) {
            zkService.start(true);
        }

        List<URI> zkUris = zkService.getServiceDetails();
        log.debug("Zookeeper service details: {}", zkUris);
        //get the zk ip details and pass it to bk, host, controller
        URI zkUri = zkUris.get(0);
        //2, check if bk is running, otherwise start, get the zk ip
        Service bkService = Utils.createBookkeeperService(zkUri);
        if (!bkService.isRunning()) {
            bkService.start(true);
        }

        List<URI> bkUris = bkService.getServiceDetails();
        log.debug("Bookkeeper service details: {}", bkUris);

        //3. start controller
        Service conService = Utils.createPravegaControllerService(zkUri);
        if (!conService.isRunning()) {
            conService.start(true);
        }

        List<URI> conUris = conService.getServiceDetails();
        log.debug("Pravega controller service details: {}", conUris);

        //4.start segmentstore
        Service segService = Utils.createPravegaSegmentStoreService(zkUri, conUris.get(0));
        if (!segService.isRunning()) {
            segService.start(true);
        }

        List<URI> segUris = segService.getServiceDetails();
        log.debug("Pravega segmentstore service details: {}", segUris);
    }

    @Before
    public void setup() {
        Service conService = Utils.createPravegaControllerService(null);
        List<URI> ctlURIs = conService.getServiceDetails();
        controllerURI = ctlURIs.get(0);
        streamManager = StreamManager.create(controllerURI);
        assertTrue("Creating Scope", streamManager.createScope(SCOPE));
        assertTrue("Creating stream", streamManager.createStream(SCOPE, STREAM, config));
    }

    @Test
    public void retentionTest() throws Exception {
        @Cleanup
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder().build());
        ControllerImpl controller = new ControllerImpl(ControllerImplConfig.builder().clientConfig(
                ClientConfig.builder().controllerURI(controllerURI).build())
                .build(),
                 connectionFactory.getInternalExecutor());
        @Cleanup
        ClientFactory clientFactory = new ClientFactoryImpl(SCOPE, controller);
        log.info("Invoking Writer test with Controller URI: {}", controllerURI);

        //create a writer
        EventStreamWriter<Serializable> writer = clientFactory.createEventWriter(STREAM,
                new JavaSerializer<>(),
                EventWriterConfig.builder().build());

        //write an event
        String writeEvent = "event";
        writer.writeEvent(writeEvent);
        writer.flush();
        log.debug("Writing event: {} ", writeEvent);

        //sleep for 4 mins
        Exceptions.handleInterrupted(() -> Thread.sleep(5 * 60 * 1000));

        //create a reader
        ReaderGroupManager groupManager = ReaderGroupManager.withScope(SCOPE, controllerURI);
        groupManager.createReaderGroup(READER_GROUP, ReaderGroupConfig.builder().disableAutomaticCheckpoints().stream(Stream.of(SCOPE, STREAM)).build());
        EventStreamReader<String> reader = clientFactory.createReader(UUID.randomUUID().toString(),
                READER_GROUP,
                new JavaSerializer<>(),
                ReaderConfig.builder().build());

        //try reading the event that was written earlier.
        //expectation is it should have been truncated and we should find stream to be empty
        assertThrows(TruncatedDataException.class, () -> reader.readNextEvent(6000));

        //verify reader functionality is unaffected post truncation
        String event = "newEvent";
        writer.writeEvent(event);
        log.info("Writing event: {}", event);
        Assert.assertEquals(event, reader.readNextEvent(6000).getEvent());

        log.debug("The stream is already truncated.Simple retention test passed.");
    }
}
