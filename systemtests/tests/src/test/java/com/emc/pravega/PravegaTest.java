/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega;

import com.emc.pravega.controller.stream.api.grpc.v1.Controller;
import com.emc.pravega.framework.Environment;
import com.emc.pravega.framework.SystemTestRunner;
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
import org.junit.Before;
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
import static org.junit.Assert.assertEquals;

@Slf4j
@RunWith(SystemTestRunner.class)
public class PravegaTest {

    private final static String STREAM_NAME = "testStreamSampleY";
    private final static String STREAM_SCOPE = "testScopeSampleY";
    private final static String READER_GROUP = "ExampleReaderGroupY";
    private final static int NUM_EVENTS = 100;
    private final ScalingPolicy scalingPolicy = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 2, 2, 4);
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

        //1. check if zk is running, if not start it
        Service zkService = new ZookeeperService("zookeeper");
        if (!zkService.isRunning()) {
            zkService.start(true);
        }

        List<URI> zkUris = zkService.getServiceDetails();
        log.debug("zookeeper service details: {}", zkUris);
        //get the zk ip details and pass it to bk, host, controller
        URI zkUri = zkUris.get(0);
        //2, check if bk is running, otherwise start, get the zk ip
        Service bkService = new BookkeeperService("bookkeeper", zkUri);
        if (!bkService.isRunning()) {
            bkService.start(true);
        }

        List<URI> bkUris = bkService.getServiceDetails();
        log.debug("bookkeeper service details: {}", bkUris);

        //3. start controller
        Service conService = new PravegaControllerService("controller", zkUri);
        if (!conService.isRunning()) {
            conService.start(true);
        }

        List<URI> conUris = conService.getServiceDetails();
        log.debug("Pravega Controller service details: {}", conUris);

        //4.start host
        Service segService = new PravegaSegmentStoreService("segmentstore", zkUri, conUris.get(0));
        if (!segService.isRunning()) {
            segService.start(true);
        }

        List<URI> segUris = segService.getServiceDetails();
        log.debug("pravega host service details: {}", segUris);
        URI segUri = segUris.get(0);
    }

    @BeforeClass
    public static void beforeClass() throws InterruptedException, ExecutionException, TimeoutException {
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
    public void createStream() throws InterruptedException, URISyntaxException, ExecutionException {

        Service conService = new PravegaControllerService("controller", null,  0, 0.0, 0.0);
        List<URI> ctlURIs = conService.getServiceDetails();
        URI controllerUri = ctlURIs.get(0);
        log.info("Invoking create stream.");
        log.info("Controller URI: {} ", controllerUri);
        ControllerImpl controller = new ControllerImpl(controllerUri.getHost(), controllerUri.getPort());
        //create a scope
        CompletableFuture<Controller.CreateScopeStatus> createScopeStatus = controller.createScope(STREAM_SCOPE);
        log.info("create scope status {}", createScopeStatus.get().getStatus());
        assertEquals(Controller.CreateScopeStatus.Status.SUCCESS, createScopeStatus.get().getStatus());
        //create a stream
        CompletableFuture<Controller.CreateStreamStatus> createStreamStatus = controller.createStream(config);
        log.info("create stream status {}", createStreamStatus.get().getStatus());
        assertEquals(Controller.CreateStreamStatus.Status.SUCCESS, createStreamStatus.get().getStatus());
    }

    /**
     * Invoke the simpleTest, ensure we are able to produce  events.
     * The test fails incase of exceptions while writing to the stream.
     *
     * @throws InterruptedException if interrupted
     * @throws URISyntaxException   If URI is invalid
     */
    @Test
    public void simpleTest() throws InterruptedException, URISyntaxException {

        Service conService = new PravegaControllerService("controller", null,  0, 0.0, 0.0);
        List<URI> ctlURIs = conService.getServiceDetails();
        URI controllerUri = ctlURIs.get(0);
        log.info("Invoking Writer test.");
        log.info("Controller URI: {} ", controllerUri);
        ClientFactory clientFactory = ClientFactory.withScope(STREAM_SCOPE, controllerUri);
        @Cleanup
        EventStreamWriter<Serializable> writer = clientFactory.createEventWriter(STREAM_NAME,
                new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        for (int i = 0; i < NUM_EVENTS; i++) {
            String event = "\n Publish \n";
            log.debug("Producing event: {} ", event);
            writer.writeEvent("", event);
            writer.flush();
            Thread.sleep(500);
        }
        log.debug("Invoking Reader test.");
        ReaderGroupManager.withScope(STREAM_SCOPE, controllerUri)
                .createReaderGroup(READER_GROUP, ReaderGroupConfig.builder().startingTime(0).build(),
                        Collections.singleton(STREAM_NAME));
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
    }
}
