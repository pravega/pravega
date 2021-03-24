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
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.Utils;
import io.pravega.test.system.framework.services.Service;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.MarathonException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;

import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Slf4j
@RunWith(SystemTestRunner.class)
public class StreamsAndScopesManagementTest extends AbstractReadWriteTest {

    private static final int NUM_SCOPES = 3;
    private static final int NUM_STREAMS = 5;
    private static final int NUM_EVENTS = 100;
    private static final int TEST_ITERATIONS = 3;
    @Rule
    public Timeout globalTimeout = Timeout.seconds(20 * 60);

    private final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(4,
            "StreamsAndScopesManagementTest-controller");

    private URI controllerURI = null;
    private StreamManager streamManager = null;
    private Controller controller;
    private Map<String, List<Long>> controllerPerfStats = new HashMap<>();

    /**
     * This is used to setup the services required by the system test framework.
     *
     * @throws MarathonException When error in setup.
     */
    @Environment
    public static void initialize() throws MarathonException, ExecutionException {
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

        final ClientConfig clientConfig = Utils.buildClientConfig(controllerURI);

        streamManager = StreamManager.create(clientConfig);
        controller = new ControllerImpl(ControllerImplConfig.builder()
                                                            .clientConfig(clientConfig)
                                                            .maxBackoffMillis(5000).build(), executor);

        // Performance inspection.
        controllerPerfStats.put("createScopeMs", new ArrayList<>());
        controllerPerfStats.put("createStreamMs", new ArrayList<>());
        controllerPerfStats.put("sealStreamMs", new ArrayList<>());
        controllerPerfStats.put("deleteStreamMs", new ArrayList<>());
        controllerPerfStats.put("deleteScopeMs", new ArrayList<>());
        controllerPerfStats.put("updateStreamMs", new ArrayList<>());
    }

    @After
    public void tearDown() {
        streamManager.close();
        ExecutorServiceHelpers.shutdown(executor);
    }

    /**
     * This test executes a series of metadata operations on streams and scopes to verify their correct behavior. This
     * includes the creation and deletion of multiple scopes both in correct and incorrect situations. Moreover, for
     * each scope, the test creates a range of streams and tries to create, update, seal and delete them in correct and
     * incorrect situations. The test also performs metadata operation on empty and non-empty streams.
     */
    @Test
    public void testStreamsAndScopesManagement() {
        // Perform management tests with Streams and Scopes.
        for (int i = 0; i < TEST_ITERATIONS; i++) {
            log.info("Stream and scope management test in iteration {}.", i);
            testStreamScopeManagementIteration();
        }

        // Provide some performance information of Stream/Scope metadata operations.
        for (String perfKey : controllerPerfStats.keySet()) {
            log.info("Performance of {}: {}", perfKey, controllerPerfStats.get(perfKey).stream().mapToLong(x -> x).summaryStatistics());
        }

        log.debug("Scope and Stream management test passed.");
    }

    // Start region utils

    private void testStreamScopeManagementIteration() {
        for (int i = 0; i < NUM_SCOPES; i++) {
            final String scope = "testStreamsAndScopesManagement" + String.valueOf(i);
            testCreateScope(scope);
            testCreateSealAndDeleteStreams(scope);
            testDeleteScope(scope);
        }
    }

    private void testCreateScope(String scope) {
        assertFalse(streamManager.deleteScope(scope));
        long iniTime = System.nanoTime();
        assertTrue("Creating scope", streamManager.createScope(scope));
        controllerPerfStats.get("createScopeMs").add(timeDiffInMs(iniTime));
    }

    private void testDeleteScope(String scope) {
        assertFalse(streamManager.createScope(scope));
        long iniTime = System.nanoTime();
        assertTrue("Deleting scope", streamManager.deleteScope(scope));
        controllerPerfStats.get("deleteScopeMs").add(timeDiffInMs(iniTime));
    }

    private void testCreateSealAndDeleteStreams(String scope) {

        final ClientConfig clientConfig = Utils.buildClientConfig(controllerURI);

        for (int j = 1; j <= NUM_STREAMS; j++) {
            final String stream = String.valueOf(j);
            StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(j)).build();

            // Create Stream with nonexistent scope, which should not be successful.
            log.info("Creating a stream in a deliberately nonexistent scope nonexistentScope/{}.", stream);
            assertThrows(RuntimeException.class, () -> streamManager.createStream("nonexistentScope", stream,
                    StreamConfiguration.builder().build()));
            long iniTime = System.nanoTime();
            log.info("Creating stream {}/{}.", scope, stream);
            assertTrue("Creating stream", streamManager.createStream(scope, stream, config));
            controllerPerfStats.get("createStreamMs").add(timeDiffInMs(iniTime));

            // Update the configuration of the stream by doubling the number of segments.
            config = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(j * 2)).build();
            iniTime = System.nanoTime();
            assertTrue(streamManager.updateStream(scope, stream, config));
            controllerPerfStats.get("updateStreamMs").add(timeDiffInMs(iniTime));

            // Perform tests on empty and non-empty streams.
            if (j % 2 == 0) {
                log.info("Writing events in stream {}/{}.", scope, stream);
                @Cleanup
                EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
                writeEvents(clientFactory, stream, NUM_EVENTS);
            }

            // Update the configuration of the stream.
            config = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(j * 2)).build();
            assertTrue(streamManager.updateStream(scope, stream, config));

            // Attempting to delete non-empty scope and non-sealed stream.
            assertThrows(RuntimeException.class, () -> streamManager.deleteScope(scope));
            assertThrows(RuntimeException.class, () -> streamManager.deleteStream(scope, stream));

            // Seal and delete stream.
            log.info("Attempting to seal and delete stream {}/{}.", scope, stream);
            iniTime = System.nanoTime();
            assertTrue(streamManager.sealStream(scope, stream));
            controllerPerfStats.get("sealStreamMs").add(timeDiffInMs(iniTime));
            iniTime = System.nanoTime();
            assertTrue(streamManager.deleteStream(scope, stream));
            controllerPerfStats.get("deleteStreamMs").add(timeDiffInMs(iniTime));

            // Seal and delete already sealed/deleted streams.
            log.info("Sealing and deleting an already deleted stream {}/{}.", scope, stream);
            assertThrows(RuntimeException.class, () -> streamManager.sealStream(scope, stream));
            assertFalse(streamManager.deleteStream(scope, stream));
        }
    }

    private long timeDiffInMs(long iniTime) {
        return (System.nanoTime() - iniTime) / 1000000;
    }

    // End region utils
}
