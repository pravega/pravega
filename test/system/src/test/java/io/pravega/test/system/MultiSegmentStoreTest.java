/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.test.system;

import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.services.BookkeeperService;
import io.pravega.test.system.framework.services.PravegaControllerService;
import io.pravega.test.system.framework.services.PravegaSegmentStoreService;
import io.pravega.test.system.framework.services.Service;
import io.pravega.test.system.framework.services.ZookeeperService;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.utils.MarathonException;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Test cases for deploying multiple segment stores.
 */
@Slf4j
@RunWith(SystemTestRunner.class)
public class MultiSegmentStoreTest {

    private Service segmentServiceInstance = null;
    private Service controllerInstance = null;

    @Environment
    public static void initialize() throws InterruptedException, MarathonException, URISyntaxException {

        // 1. Check if zk is running, if not start it.
        Service zkService = new ZookeeperService("zookeeper");
        if (!zkService.isRunning()) {
            zkService.start(true);
        }

        List<URI> zkUris = zkService.getServiceDetails();
        log.info("zookeeper service details: {}", zkUris);
        URI zkUri = zkUris.get(0);
        // 2. Check if bk is running, otherwise start it.
        Service bkService = new BookkeeperService("bookkeeper", zkUri);
        if (!bkService.isRunning()) {
            bkService.start(true);
        }

        List<URI> bkUris = bkService.getServiceDetails();
        log.info("bookkeeper service details: {}", bkUris);

        // 3. Start controller.
        Service controllerService = new PravegaControllerService("controller", zkUri);
        if (!controllerService.isRunning()) {
            controllerService.start(true);
        }

        List<URI> conUris = controllerService.getServiceDetails();
        log.info("Pravega Controller service instance details: {}", conUris);

        // 4. Start segment store.
        Service segService = new PravegaSegmentStoreService("segmentstore", zkUri, conUris.get(0));
        if (!segService.isRunning()) {
            segService.start(true);
        }

        List<URI> segUris = segService.getServiceDetails();
        log.info("pravega host service details: {}", segUris);
    }

    @Before
    public void setup() {
        Service zkService = new ZookeeperService("zookeeper");
        Assert.assertTrue(zkService.isRunning());
        List<URI> zkUris = zkService.getServiceDetails();
        log.info("zookeeper service details: {}", zkUris);

        // Verify controller is running.
        this.controllerInstance = new PravegaControllerService("controller", zkUris.get(0));
        Assert.assertTrue(this.controllerInstance.isRunning());
        List<URI> conURIs = this.controllerInstance.getServiceDetails();
        log.info("Pravega Controller service instance details: {}", conURIs);

        // Verify segment stores is running.
        this.segmentServiceInstance = new PravegaSegmentStoreService("segmentstore", zkUris.get(0), conURIs.get(0));
        Assert.assertTrue(this.segmentServiceInstance.isRunning());
        Assert.assertEquals(1, this.segmentServiceInstance.getServiceDetails().size());
        log.info("Pravega segment store instance details: {}", this.segmentServiceInstance.getServiceDetails());
    }

    @After
    public void tearDown() {
        // Scale back the segment store instance count.
        this.segmentServiceInstance.scaleService(1, true);
    }

    @Test(timeout = 600000)
    public void testMultiSegmentStores() throws InterruptedException {
        // Test Sanity.
        log.info("Test with 1 segment store running");
        testReadWrite();

        // Scale up and test.
        this.segmentServiceInstance.scaleService(3, true);
        // Wait for all containers to be up and registered.
        Thread.sleep(60000);
        log.info("Test with 3 segment stores running");
        testReadWrite();

        // Rescale and verify.
        this.segmentServiceInstance.scaleService(2, true);
        // Wait for all containers to be up and registered.
        Thread.sleep(60000);
        log.info("Test with 2 segment stores running");
        testReadWrite();

        // Rescale to single instance and verify.
        this.segmentServiceInstance.scaleService(1, true);
        // Wait for all containers to be up and registered.
        Thread.sleep(60000);
        log.info("Test with 1 segment store running");
        testReadWrite();
    }

    private void testReadWrite() {
        List<URI> ctlURIs = this.controllerInstance.getServiceDetails();
        URI controllerUri = ctlURIs.get(0);

        String scope = "testscope" + RandomStringUtils.randomAlphanumeric(10);
        String stream = "teststream" + RandomStringUtils.randomAlphanumeric(10);

        @Cleanup
        StreamManager streamManager = StreamManager.create(controllerUri);
        Assert.assertTrue(streamManager.createScope(scope));

        // Create stream with large number of segments so that most segment containers are used.
        Assert.assertTrue(streamManager.createStream(scope, stream, StreamConfiguration.builder()
                .scope(scope)
                .streamName(stream)
                .retentionPolicy(RetentionPolicy.INFINITE)
                .scalingPolicy(ScalingPolicy.fixed(10))
                .build()));

        @Cleanup
        ClientFactory clientFactory = ClientFactory.withScope(scope, controllerUri);

        log.info("Invoking writer with controller URI: {}", controllerUri);
        @Cleanup
        EventStreamWriter<Serializable> writer = clientFactory.createEventWriter(stream,
                new JavaSerializer<>(),
                EventWriterConfig.builder().build());

        final int numEvents = 1000;
        final String fixedEvent = "testevent";
        for (int i = 0; i < numEvents; i++) {
            log.debug("Producing event: {} ", fixedEvent);
            writer.writeEvent(String.valueOf(i), fixedEvent);
        }
        writer.flush();

        log.info("Invoking reader with controller URI: {}", controllerUri);
        final String readerGroup = "testreadergroup" + RandomStringUtils.randomAlphanumeric(10);
        ReaderGroupManager.withScope(scope, controllerUri)
                .createReaderGroup(readerGroup, ReaderGroupConfig.builder().startingTime(0).build(),
                        Collections.singleton(stream));

        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader(UUID.randomUUID().toString(),
                readerGroup,
                new JavaSerializer<>(),
                ReaderConfig.builder().build());
        for (int i = 0; i < numEvents; i++) {
            try {
                String event = reader.readNextEvent(60000).getEvent();
                Assert.assertEquals(fixedEvent, event);
            } catch (ReinitializationRequiredException e) {
                log.error("Unexpected request to reinitialize {}", e);
                throw new IllegalStateException("Unexpected request to reinitialize");
            }
        }
    }
}
