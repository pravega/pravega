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
import io.pravega.client.control.impl.Controller;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.shared.NameUtils;
import io.pravega.test.common.ThreadPooledTestSuite;
import io.pravega.test.integration.PravegaResource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.Timeout;

import static org.junit.Assert.assertTrue;

public class AbstractEndToEndTest extends ThreadPooledTestSuite {
    
    @ClassRule
    public static final PravegaResource PRAVEGA = new PravegaResource();
    
    protected static final String SCOPE = "testScope";

    @Rule
    public Timeout globalTimeout = Timeout.seconds(60);

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


    protected void createScope(final String scopeName) {
        Controller controller = PRAVEGA.getLocalController();
        controller.createScope(scopeName).join();
    }

    protected void createStream(final String scopeName, final String streamName, final ScalingPolicy scalingPolicy) {
        Controller controller = PRAVEGA.getLocalController();
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

    protected Segment getSegment(String streamName, int segmentNumber, int epoch) {
        return new Segment(SCOPE, streamName, NameUtils.computeSegmentId(segmentNumber, epoch));
    }

    protected void scaleStream(final String streamName, final Map<Double, Double> keyRanges) throws Exception {
        Stream stream = Stream.of(SCOPE, streamName);
        Controller controller = PRAVEGA.getLocalController();
        List<Long> currentSegments = controller.getCurrentSegments(SCOPE, streamName).join().getSegments()
                                               .stream().map(Segment::getSegmentId).collect(Collectors.toList());
        assertTrue(controller.scaleStream(stream, currentSegments, keyRanges, executorService()).getFuture().get());
    }
}
