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
package io.pravega.test.integration.endtoendtest;

import com.google.common.collect.ImmutableMap;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.common.Exceptions;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.shared.NameUtils;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.common.ThreadPooledTestSuite;
import io.pravega.test.integration.demo.ControllerWrapper;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Cleanup;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.Timeout;

import static org.junit.Assert.assertTrue;

public class AbstractEndToEndTest extends ThreadPooledTestSuite {
    protected static final String SCOPE = "testScope";
    protected static final String STREAM = "testStream1";

    @Rule
    public Timeout globalTimeout = Timeout.seconds(60);
    
    protected final int controllerPort = TestUtils.getAvailableListenPort();
    protected final String serviceHost = "localhost";
    protected final URI controllerURI = URI.create("tcp://" + serviceHost + ":" + controllerPort);
    protected final int servicePort = TestUtils.getAvailableListenPort();
    protected final int containerCount = 4;
    protected TestingServer zkTestServer;
    protected PravegaConnectionListener server;
    protected ControllerWrapper controllerWrapper;
    protected ServiceBuilder serviceBuilder;
    protected final Serializer<String> serializer = new JavaSerializer<>();
    final Random random = new Random();
    final Supplier<String> randomKeyGenerator = () -> String.valueOf(random.nextInt());
    //Map with has a mapping of routing key to its corresponding key.
    final Map<String, String> keyReverseMap = ImmutableMap.<String, String>builder().put("0.1", "14")
                                                                                    .put("0.2", "11")
                                                                                    .put("0.3", "2")
                                                                                    .put("0.4", "1")
                                                                                    .put("0.5", "10")
                                                                                    .put("0.6", "3")
                                                                                    .put("0.7", "5")
                                                                                    .put("0.8", "7")
                                                                                    .put("0.9", "6")
                                                                                    .put("1.0", "4")
                                                                                    .build();
    final Function<String, String> keyGenerator = routingKey -> keyReverseMap.getOrDefault(routingKey, "0.1");
    final Function<Integer, String> getEventData = eventNumber -> String.valueOf(eventNumber) + ":constant data"; //event

    @Before
    public void setUp() throws Exception {
        zkTestServer = new TestingServerStarter().start();

        serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();

        server = new PravegaConnectionListener(false, servicePort, store, tableStore, this.serviceBuilder.getLowPriorityExecutor());
        server.startListening();

        controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(),
                                                  false,
                                                  controllerPort,
                                                  serviceHost,
                                                  servicePort,
                                                  containerCount);
        controllerWrapper.awaitRunning();
    }

    @After
    public void tearDown() throws Exception {
        controllerWrapper.close();
        server.close();
        serviceBuilder.close();
        zkTestServer.close();
    }

    protected void createScope(final String scopeName) {
        @Cleanup
        Controller controller = Exceptions.handleInterruptedCall(controllerWrapper::getController);
        controller.createScope(scopeName).join();
    }

    protected void createStream(final String scopeName, final String streamName, final ScalingPolicy scalingPolicy) {
        @Cleanup
        Controller controller = Exceptions.handleInterruptedCall(controllerWrapper::getController);
        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scalingPolicy(scalingPolicy)
                                                        .build();
        controller.createStream(scopeName, streamName, config).join();
    }

    protected void readAndVerify(final EventStreamReader<String> reader, int...eventIds) throws ReinitializationRequiredException {
        ArrayList<String> results = new ArrayList<>(eventIds.length);

        // Attempt reading eventIds.length events
        for (int eventId : eventIds) {
            String event = reader.readNextEvent(15000).getEvent();
            while (event == null) { //try until a non null event is read
                event = reader.readNextEvent(15000).getEvent();
            }
            results.add(event);
        }

        //Verify if we have received the events according to the event ids provided.
        Arrays.stream(eventIds).forEach(i -> assertTrue(results.contains(getEventData.apply(i))));
    }

    protected Segment getSegment(int segmentNumber, int epoch) {
        return new Segment(SCOPE, STREAM, NameUtils.computeSegmentId(segmentNumber, epoch));
    }

    protected void scaleStream(final String streamName, final Map<Double, Double> keyRanges) throws Exception {
        Stream stream = Stream.of(SCOPE, streamName);
        Controller controller = controllerWrapper.getController();
        List<Long> currentSegments = controller.getCurrentSegments(SCOPE, streamName).join().getSegments()
                                               .stream().map(Segment::getSegmentId).collect(Collectors.toList());
        assertTrue(controller.scaleStream(stream, currentSegments, keyRanges, executorService()).getFuture().get());
    }
}
