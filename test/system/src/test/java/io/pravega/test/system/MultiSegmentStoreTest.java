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
package io.pravega.test.system;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.common.concurrent.Futures;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.Utils;
import io.pravega.test.system.framework.services.Service;
import java.io.Serializable;
import java.net.URI;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.MarathonException;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;

/**
 * Test cases for deploying multiple segment stores.
 */
@Slf4j
@RunWith(SystemTestRunner.class)
public class MultiSegmentStoreTest extends AbstractSystemTest {

    @Rule
    public Timeout globalTimeout = Timeout.seconds(10 * 60);

    private Service segmentServiceInstance = null;
    private Service controllerInstance = null;

    @Environment
    public static void initialize() throws MarathonException {
        URI zkUri = startZookeeperInstance();
        startBookkeeperInstances(zkUri);
        URI controllerUri = ensureControllerRunning(zkUri);
        ensureSegmentStoreRunning(zkUri, controllerUri);
    }

    @Before
    public void setup() {
        Service zkService = Utils.createZookeeperService();
        Assert.assertTrue(zkService.isRunning());
        List<URI> zkUris = zkService.getServiceDetails();
        log.info("zookeeper service details: {}", zkUris);

        // Verify controller is running.
        this.controllerInstance = Utils.createPravegaControllerService(zkUris.get(0));
        Assert.assertTrue(this.controllerInstance.isRunning());
        List<URI> conURIs = this.controllerInstance.getServiceDetails();
        log.info("Pravega Controller service instance details: {}", conURIs);

        // Verify segment stores is running.
        this.segmentServiceInstance = Utils.createPravegaSegmentStoreService(zkUris.get(0), conURIs.get(0));
        Assert.assertTrue(this.segmentServiceInstance.isRunning());
        Assert.assertEquals(1, this.segmentServiceInstance.getServiceDetails().size());
        log.info("Pravega segment store instance details: {}", this.segmentServiceInstance.getServiceDetails());
    }

    @After
    public void tearDown() throws ExecutionException {
        // Scale back the segment store instance count.
        Futures.getAndHandleExceptions(this.segmentServiceInstance.scaleService(1), ExecutionException::new);
    }

    @Test
    public void testMultiSegmentStores() throws InterruptedException, ExecutionException {
        // Test Sanity.
        log.info("Test with 1 segment store running");
        testReadWrite();

        // Scale up and test.
        Futures.getAndHandleExceptions(this.segmentServiceInstance.scaleService(3), ExecutionException::new);
        // Wait for all containers to be up and registered.
        Thread.sleep(60000);
        log.info("Test with 3 segment stores running");
        testReadWrite();

        // Rescale and verify.
        Futures.getAndHandleExceptions(this.segmentServiceInstance.scaleService(2), ExecutionException::new);
        // Wait for all containers to be up and registered.
        Thread.sleep(60000);
        log.info("Test with 2 segment stores running");
        testReadWrite();

        // Rescale to single instance and verify.
        Futures.getAndHandleExceptions(this.segmentServiceInstance.scaleService(1), ExecutionException::new);
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

        ClientConfig clientConfig = Utils.buildClientConfig(controllerUri);
        @Cleanup
        StreamManager streamManager = StreamManager.create(clientConfig);
        Assert.assertTrue(streamManager.createScope(scope));

        // Create stream with large number of segments so that most segment containers are used.
        Assert.assertTrue(streamManager.createStream(scope, stream, StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(10))
                .build()));

        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope,
                clientConfig);

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
        ReaderGroupManager groupManager = ReaderGroupManager.withScope(scope, clientConfig);
        groupManager.createReaderGroup(readerGroup,
                ReaderGroupConfig.builder().disableAutomaticCheckpoints().stream(Stream.of(scope, stream)).build());

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
