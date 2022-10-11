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
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.Utils;
import io.pravega.test.system.framework.services.Service;

import java.net.URI;
import java.util.List;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;

import static org.apache.commons.lang.RandomStringUtils.randomAlphanumeric;
import static org.junit.Assert.assertTrue;

@Slf4j
@RunWith(SystemTestRunner.class)
public class CacheSegmentToHostMappingPerformanceTest extends AbstractReadWriteTest {
    private final static String STREAM_NAME = "testStreamSampleA";
    private final static String STREAM_SCOPE = "testScopeSampleA" + randomAlphanumeric(5);

    @Rule
    public Timeout globalTimeout = Timeout.seconds(5 * 60);

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
     * Invoke the simpleTest, to see the difference in performance for reading
     * segment endpoint from cache v/s Network.
     */
    @Test
    public void simpleTest() {
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

        assertTrue(controller.createScope(STREAM_SCOPE).join());
        assertTrue(controller.createStream(STREAM_SCOPE, STREAM_NAME, config).join());

        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(STREAM_SCOPE, Utils.buildClientConfig(controllerUri));
        log.info("Invoking Writer test with Controller URI: {}", controllerUri);

        long networkReadDuration = testRunDurationForSegmentEndpointRead(controller, "testStreamSampleB", 3, "Network");
        long cacheReadDuration = testRunDurationForSegmentEndpointRead(controller, "testStreamSampleC", 4, "Cache");
        log.info("Time to read data from network is : {} and from Cache is : {} ", networkReadDuration, cacheReadDuration);
        Assert.assertTrue("The endpoint read duration assertion failed!", networkReadDuration  > cacheReadDuration );

    }

    private long testRunDurationForSegmentEndpointRead(ControllerImpl controller, String streamName, int id,  String dataSource) {
        Segment segment = new Segment(STREAM_SCOPE, streamName, id);
        long start = System.currentTimeMillis();
        for (int i = 0; i < 500; i++) {
            try {
                if (dataSource.equalsIgnoreCase("Network")) {
                    controller.getPravegaNodeUri(segment).join();
                } else {
                    controller.getEndpointForSegment(segment.getScopedName()).join();
                }
            } catch (Exception e) {
                log.info("Exception while getting segment end point data read");
            }
        }
        return System.currentTimeMillis() - start;
    }
}
