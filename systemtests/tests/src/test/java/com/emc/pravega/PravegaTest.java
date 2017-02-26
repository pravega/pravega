/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega;

import com.emc.pravega.controller.stream.api.v1.CreateScopeStatus;
import com.emc.pravega.controller.stream.api.v1.CreateStreamStatus;
import com.emc.pravega.framework.Environment;
import com.emc.pravega.framework.SystemTestRunner;
import com.emc.pravega.framework.metronome.AuthEnabledMetronomeClient;
import com.emc.pravega.framework.services.BookkeeperService;
import com.emc.pravega.framework.services.PravegaControllerService;
import com.emc.pravega.framework.services.PravegaSegmentStoreService;
import com.emc.pravega.framework.services.Service;
import com.emc.pravega.framework.services.ZookeeperService;
import com.emc.pravega.stream.EventStreamReader;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.ReaderConfig;
import com.emc.pravega.stream.ReaderGroupConfig;
import com.emc.pravega.stream.ReinitializationRequiredException;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.ControllerImpl;
import com.emc.pravega.stream.impl.JavaSerializer;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.utils.MarathonException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static com.emc.pravega.framework.metronome.AuthEnabledMetronomeClient.getClient;
import static org.junit.Assert.assertEquals;

@Slf4j
@RunWith(SystemTestRunner.class)
public class PravegaTest {

    private final static String STREAM_NAME = "testStreamSample";
    private final static String STREAM_SCOPE = "testScopeSample";
    private final static String READER_GROUP = "ExampleReaderGroup";
    private final ScalingPolicy scalingPolicy = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 2L, 2, 2);
    private final StreamConfiguration config = StreamConfiguration.builder().scope(STREAM_SCOPE).streamName(STREAM_NAME).scalingPolicy(scalingPolicy).build();

    /**
     * This is used to setup the various services required by the system test framework.
     *
     * @throws InterruptedException If interrupted
     * @throws MarathonException    when error in setup
     * @throws URISyntaxException   If URI is invalid
     */
    @Environment
    public static void setup() throws InterruptedException, MarathonException, URISyntaxException {
        AuthEnabledMetronomeClient.deleteAllJobs(getClient());
        //1. check if zk is running, if not start it
        Service zkService = new ZookeeperService("zookeeper");
        if (!zkService.isRunning()) {
            if (!zkService.isStaged()) {
                zkService.start(true);
            }
        }

        List<URI> zkUris = zkService.getServiceDetails();
        log.debug("zookeeper service details: {}", zkUris);
        //get the zk ip details and pass it to bk, host, controller
        URI zkUri = zkUris.get(0);

        //2, check if bk is running, otherwise start, get the zk ip
        Service bkService = new BookkeeperService("bookkeeper", zkUri, 3, 0.5, 512.0);
        if (!bkService.isRunning()) {
            if (!bkService.isStaged()) {
                bkService.start(true);
            }
        }

        List<URI> bkUris = bkService.getServiceDetails();
        log.debug("bookkeeper service details: {}", bkUris);

        //4.start host
        Service segService = new PravegaSegmentStoreService("host", zkUri, 1, 1, 512.0);

        if (!segService.isRunning()) {
            if (!segService.isStaged()) {
                segService.start(true);
            }
        }

        List<URI> segUris = segService.getServiceDetails();
        log.debug("pravega host service details: {}", segUris);
        URI segUri = segUris.get(0);

        //3. start controller
        Service conService = new PravegaControllerService("controller", zkUri, segUri, 1, 0.1, 256);
        if (!conService.isRunning()) {
            if (!conService.isStaged()) {
                conService.start(true);
            }
        }

        List<URI> conUris = conService.getServiceDetails();
        log.debug("Pravega Controller service details: {}", conUris);

    }

    @BeforeClass
    public static void beforeClass() throws InterruptedException, ExecutionException, TimeoutException {
        // This is the placeholder to perform any operation on the services before executing the system tests
    }

    /**
     * Invoke the create stream test, ensure we are able to create scope and stream.
     * The test fails incase of exceptions in creating scope or stream
     *
     * @throws InterruptedException if interrupted
     * @throws URISyntaxException   If URI is invalid
     */
    @Test
    public  void createStreamTest() throws InterruptedException, URISyntaxException {

        Service conService = new PravegaControllerService("controller", null, null, 0, 0.0, 0.0);
        List<URI> ctlURIs = conService.getServiceDetails();
        URI controllerUri = ctlURIs.get(0);

        log.debug("Invoking create stream test.");

        log.debug("Controller URI: {} ", controllerUri);

        ControllerImpl controller = new ControllerImpl(controllerUri.getHost(), controllerUri.getPort());

        try {
            //create a scope
            CompletableFuture<CreateScopeStatus> scopeStatus = controller.createScope(STREAM_SCOPE);
            log.debug("create scope status {}", scopeStatus.get());
            assertEquals("SUCCESS", scopeStatus.get());
            //create a stream
            CompletableFuture<CreateStreamStatus> status = controller.createStream(config);
            log.debug("create stream status {}", status.get());
            assertEquals("SUCCESS", status.get());
        } catch (ExecutionException e) {
            log.error("error in doing a get on create stream status {}", e);
            System.exit(0);
        }

        Thread.sleep(30000);

    }

    /**
     * Invoke the producer test, ensure we are able to produce 100 messages to the stream.z
     * The test fails incase of exceptions while writing to the stream.
     *
     * @throws InterruptedException if interrupted
     * @throws URISyntaxException   If URI is invalid
     */
    @Test
    public void producerTest() throws InterruptedException, URISyntaxException {

        Service conService = new PravegaControllerService("controller", null, null, 0, 0.0, 0.0);
        List<URI> ctlURIs = conService.getServiceDetails();
        URI controllerUri = ctlURIs.get(0);

        log.debug("Invoking producer test.");

        log.debug("Controller URI: {} ", controllerUri);

        ClientFactory clientFactory = ClientFactory.withScope(STREAM_SCOPE, controllerUri);

        @Cleanup
        EventStreamWriter<Serializable> producer = clientFactory.createEventWriter(STREAM_NAME,
                new JavaSerializer<>(),
                EventWriterConfig.builder().build());

        for (int i = 0; i < 5; i++) {
            String event = "\n Transactional Publish \n";
            log.debug("Producing event: {} ", event);
            producer.writeEvent("", event);
            producer.flush();
            Thread.sleep(2000);
        }
        Thread.sleep(10000);
    }

    /**
     * Invoke consumer test, ensure we are able to read 100 messages from the stream.
     * The test fails incase of exceptions/ timeout.
     *
     * @throws URISyntaxException If URI is invalid
     */
    @Test
    public void consumerTest() throws URISyntaxException {

        Service conService = new PravegaControllerService("controller", null, null, 0, 0.0, 0.0);
        List<URI> ctlURIs = conService.getServiceDetails();
        URI controllerUri = ctlURIs.get(0);

        log.debug("Invoking consumer test.");
        log.debug("Controller URI: " + controllerUri);

        ClientFactory clientFactory = ClientFactory.withScope(STREAM_SCOPE, controllerUri);

        ReaderGroupManager.withScope(STREAM_SCOPE, controllerUri)
                .createReaderGroup(READER_GROUP, ReaderGroupConfig.builder().startingTime(0).build(),
                        Collections.singletonList(STREAM_NAME));

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            log.error(" error in thread sleep {}", e);
        }

        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader(UUID.randomUUID().toString(),
                READER_GROUP,
                new JavaSerializer<>(),
                ReaderConfig.builder().build());
        for (int i = 0; i < 5; i++) {
            String event = null;
            try {
                event = reader.readNextEvent(6000).getEvent();
            } catch (ReinitializationRequiredException e) {
                log.error(" error in reading next event with a given timeout{}", e);
            }
            log.debug("Read event: {} ", event);
        }
        reader.close();
        System.exit(0);

    }
}
