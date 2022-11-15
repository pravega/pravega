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

import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ByteBufferSerializer;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.Utils;
import io.pravega.test.system.framework.services.Service;

import java.lang.management.ManagementFactory;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;

import static org.apache.commons.lang.RandomStringUtils.randomAlphanumeric;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
@RunWith(SystemTestRunner.class)
public class LargeEventTest extends AbstractReadWriteTest {

    private final static int NUM_EVENTS = 10;

    @Rule
    public Timeout globalTimeout = Timeout.seconds(5 * 600);

    private final ScalingPolicy scalingPolicy = ScalingPolicy.fixed(4);
    private final StreamConfiguration config = StreamConfiguration.builder()
                                                                  .scalingPolicy(scalingPolicy)
                                                                  .build();

    /**
     * This is used to setup the various services required by the system test framework.
     */
    @Environment
    public static void initialize() {
        URI zkUri = startZookeeperInstance();
        startBookkeeperInstances(zkUri);
        URI controllerUri = ensureControllerRunning(zkUri);
        ensureSegmentStoreRunning(zkUri, controllerUri);
    }

    /**
     * Invoke the largeEventSimpleTest, ensure we are able to produce  events.
     * The test fails in case of exceptions while writing to the stream.
     */
    @Test
    public void largeEventSimpleTest() {
        String streamScope = "testScopeSampleY" + randomAlphanumeric(5);
        for (int tests = 0; tests < NUM_EVENTS; tests++) {
            log.info("Heap memory {}", ManagementFactory.getMemoryMXBean().getHeapMemoryUsage());
            log.info("Non-heap memory {}", ManagementFactory.getMemoryMXBean().getHeapMemoryUsage());

            String streamName = "testLargeEventStream" + tests;
            String readerGroupName = "testLargeEventReaderGroup" + tests;

            Service conService = Utils.createPravegaControllerService(null);
            List<URI> ctlURIs = conService.getServiceDetails();
            URI controllerUri = ctlURIs.get(0);

            log.info("Invoking create stream with Controller URI: {}", controllerUri);

            @Cleanup
            ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(Utils.buildClientConfig(controllerUri));
            @Cleanup
            ControllerImpl controller = new ControllerImpl(ControllerImplConfig.builder()
                    .clientConfig(Utils.buildClientConfig(controllerUri))
                    .build(), connectionFactory.getInternalExecutor());

            controller.createScope(streamScope).join();
            assertTrue(controller.createStream(streamScope, streamName, config).join());

            @Cleanup
            EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(streamScope, Utils.buildClientConfig(controllerUri));
            log.info("Invoking Writer test with Controller URI: {}", controllerUri);

            @Cleanup
            EventStreamWriter<ByteBuffer> writer = clientFactory.createEventWriter(streamName,
                    new ByteBufferSerializer(),
                    EventWriterConfig.builder().enableLargeEvents(true).build());
            byte[] payload = new byte[Serializer.MAX_EVENT_SIZE * 10];
            for (int i = 0; i < NUM_EVENTS; i++) {
                log.info("Heap memory {}", ManagementFactory.getMemoryMXBean().getHeapMemoryUsage());
                log.info("Non-heap memory {}", ManagementFactory.getMemoryMXBean().getHeapMemoryUsage());
                log.info("Writing event: {} ", i);
                // any exceptions while writing the event will fail the test.
                writer.writeEvent("", ByteBuffer.wrap(payload)).join();
                log.info("Wrote event: {} ", i);
                writer.flush();
            }

            log.info("Invoking Reader test.");
            @Cleanup
            ReaderGroupManager groupManager = ReaderGroupManager.withScope(streamScope, Utils.buildClientConfig(controllerUri));
            groupManager.createReaderGroup(readerGroupName, ReaderGroupConfig.builder().stream(Stream.of(streamScope, streamName)).build());
            @Cleanup
            EventStreamReader<ByteBuffer> reader = clientFactory.createReader(UUID.randomUUID().toString(),
                    readerGroupName,
                    new ByteBufferSerializer(),
                    ReaderConfig.builder().build());
            int readCount = 0;

            EventRead<ByteBuffer> event;
            do {
                event = reader.readNextEvent(10_000);
                log.debug("Read event: {}.", event.getEvent());
                if (event.getEvent() != null) {
                    readCount++;
                }
                // try reading until all the written events are read, else the test will timeout.
            } while ((event.getEvent() != null || event.isCheckpoint()) && readCount < NUM_EVENTS);
            assertEquals("Read count should be equal to write count", NUM_EVENTS, readCount);
        }
    }
}
