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
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
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
import java.util.concurrent.CompletableFuture;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.MarathonException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
@RunWith(SystemTestRunner.class)
public class RetentionTest extends AbstractSystemTest {

    private static final String SCOPE = "testRetentionScope" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    private static final String STREAM_TIME = "testRetentionStreamTime";
    private static final String STREAM_SIZE = "testRetentionStreamSize";
    private static final String READER_GROUP = "testRetentionReaderGroup" + RandomFactory.create().nextInt(Integer.MAX_VALUE);

    @Rule
    public Timeout globalTimeout = Timeout.seconds(8 * 60);

    private final ScalingPolicy scalingPolicy = ScalingPolicy.fixed(2);
    private final RetentionPolicy retentionPolicyTime = RetentionPolicy.byTime(Duration.ofMinutes(1));
    private final RetentionPolicy retentionPolicySize = RetentionPolicy.bySizeBytes(1);
    private final StreamConfiguration configTime = StreamConfiguration.builder().scalingPolicy(scalingPolicy).retentionPolicy(retentionPolicyTime).build();
    private final StreamConfiguration configSize = StreamConfiguration.builder().scalingPolicy(scalingPolicy)
                                                                      .retentionPolicy(retentionPolicySize).build();
    private URI controllerURI;
    private StreamManager streamManager;

    /**
     * This is used to setup the various services required by the system test framework.
     *
     * @throws MarathonException    when error in setup
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
        assertTrue("Creating Scope", streamManager.createScope(SCOPE));
        assertTrue("Creating stream", streamManager.createStream(SCOPE, STREAM_TIME, configTime));
        assertTrue("Creating stream", streamManager.createStream(SCOPE, STREAM_SIZE, configSize));
    }

    @After
    public void tearDown() {
        streamManager.close();
    }

    @Test
    public void retentionTest() throws Exception {
        CompletableFuture.allOf(retentionTest(STREAM_TIME, false), retentionTest(STREAM_SIZE, true));
    }
    
    private CompletableFuture<Void> retentionTest(String streamName, boolean sizeBased) throws Exception {
        return CompletableFuture.runAsync(() -> {
            final ClientConfig clientConfig = Utils.buildClientConfig(controllerURI);
            @Cleanup
            ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(clientConfig);
            ControllerImpl controller = new ControllerImpl(ControllerImplConfig.builder().clientConfig(clientConfig).build(),
                    connectionFactory.getInternalExecutor());
            @Cleanup
            ClientFactoryImpl clientFactory = new ClientFactoryImpl(SCOPE, controller, connectionFactory);
            log.info("Invoking Writer test with Controller URI: {}", controllerURI);

            //create a writer
            @Cleanup
            EventStreamWriter<Serializable> writer = clientFactory.createEventWriter(streamName,
                    new JavaSerializer<>(),
                    EventWriterConfig.builder().build());

            //write an event
            String writeEvent = "event";
            writer.writeEvent(writeEvent);
            if (sizeBased) {
                // since truncation always happens at an event boundary, for size based, we will write two events, 
                // so that truncation can happen at the first event. 
                writer.writeEvent(writeEvent);
            }
            writer.flush();
            log.debug("Writing event: {} ", writeEvent);

            // sleep for 5 mins -- retention frequency is set to 2 minutes. So in 5 minutes we should definitely have 
            // 2 retention cycles, with a stream cut being computed in first cycle and truncation happening on the 
            // previously computed streamcut in second cycle. 
            // for time based retention, we wrote one event, which would get truncated. 
            // for size based retention we wrote two events such that stream would retain at least 1 byte as prescribed by 
            // the policy
            Exceptions.handleInterrupted(() -> Thread.sleep(5 * 60 * 1000));

            //create a reader
            ReaderGroupManager groupManager = ReaderGroupManager.withScope(SCOPE, clientConfig);
            String groupName = READER_GROUP + streamName;
            groupManager.createReaderGroup(groupName, 
                    ReaderGroupConfig.builder().disableAutomaticCheckpoints().stream(Stream.of(SCOPE, streamName)).build());
            EventStreamReader<String> reader = clientFactory.createReader(UUID.randomUUID().toString(),
                    groupName,
                    new JavaSerializer<>(),
                    ReaderConfig.builder().build());

            if (sizeBased) {
                // we should read one write event back from the stream. 
                String event = reader.readNextEvent(6000).getEvent();
                assertEquals(event, writeEvent);
            }
            //verify reader functionality is unaffected post truncation
            String event = "newEvent";
            writer.writeEvent(event);
            log.info("Writing event: {}", event);
            Assert.assertEquals(event, reader.readNextEvent(6000).getEvent());

            log.debug("The stream is already truncated.Simple retention test passed.");
        });
    }
}
