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

import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.shared.NameUtils;
import io.pravega.test.system.framework.SystemTestRunner;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * This test creates a stream with 10k segments and then rapidly scales it 10 times.
 * Then it performs truncation a random number of times.
 */
@Slf4j
@RunWith(SystemTestRunner.class)
public class MetadataScalabilityLargeNumSegmentsTest extends MetadataScalabilityTest {
    private static final String STREAM_NAME = "metadataScalabilitySegments";
    private static final int NUM_SEGMENTS = 10000;
    private static final StreamConfiguration CONFIG = StreamConfiguration.builder()
                                                                         .scalingPolicy(ScalingPolicy.fixed(NUM_SEGMENTS)).build();
    private static final int SCALES_TO_PERFORM = 10;
    
    private final AtomicInteger counter = new AtomicInteger(0);

    @Override
    String getStreamName() {
        return STREAM_NAME;
    }

    @Override
    StreamConfiguration getStreamConfig() {
        return CONFIG;
    }
    
    @Override
    int getScalesToPerform() {
        return SCALES_TO_PERFORM;
    }

    /**
     * Chooses one segment out of the current segments and selects its matching range as the input for next scale.
     * @param sortedCurrentSegments sorted current segments
     * @return scale input for next scale
     */
    @Override
    Pair<List<Long>, Map<Double, Double>> getScaleInput(ArrayList<Segment> sortedCurrentSegments) {
        int i = counter.incrementAndGet();
        List<Long> segmentsToSeal = sortedCurrentSegments.stream()
                                          .filter(x -> i - 1 == NameUtils.getSegmentNumber(x.getSegmentId()) % NUM_SEGMENTS)
                                          .map(Segment::getSegmentId).collect(Collectors.toList());
        Map<Double, Double> newRanges = new HashMap<>();
        double delta = 1.0 / NUM_SEGMENTS;
        newRanges.put(delta * (i - 1), delta * i);

        return new ImmutablePair<>(segmentsToSeal, newRanges);
    }

    @Test
    public void largeNumSegmentsScalability() {
        testState = new TestState(false);

        ControllerImpl controller = getController();

        List<List<Segment>> listOfEpochs = scale(controller);
        truncation(controller, listOfEpochs);
        sealAndDeleteStream(controller);
    }
}
