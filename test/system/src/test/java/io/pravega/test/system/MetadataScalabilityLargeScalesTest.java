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
import io.pravega.test.system.framework.SystemTestRunner;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This test creates a stream with 10 segments and then rapidly scales it 1010 times.
 * Then it performs truncation a random number of times.
 */
@Slf4j
@RunWith(SystemTestRunner.class)
public class MetadataScalabilityLargeScalesTest extends MetadataScalabilityTest {
    private static final String STREAM_NAME = "metadataScalabilityScale";
    private static final int NUM_SEGMENTS = 10;
    private static final StreamConfiguration CONFIG = StreamConfiguration.builder()
                                                                         .scalingPolicy(ScalingPolicy.fixed(NUM_SEGMENTS)).build();
    private static final int SCALES_TO_PERFORM = 1010;
    
    private final Map<Double, Double> newRanges = new HashMap<>();
    
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
     * Scale all the segments in the current epoch and replace them with new identical 10 segments. 
     * @param sortedCurrentSegments segments in current epoch
     * @return scale input for next scale to perform
     */
    @Override
    Pair<List<Long>, Map<Double, Double>> getScaleInput(ArrayList<Segment> sortedCurrentSegments) {
        return new ImmutablePair<>(getSegmentsToSeal(sortedCurrentSegments), getNewRanges());     
    }
    
    private List<Long> getSegmentsToSeal(ArrayList<Segment> sorted) {
        return sorted.stream()
                     .map(Segment::getSegmentId).collect(Collectors.toList());
    }

    @Synchronized
    private Map<Double, Double> getNewRanges() {
        if (newRanges.isEmpty()) {
            double delta = 1.0 / NUM_SEGMENTS;
            for (int i = 0; i < NUM_SEGMENTS; i++) {
                double low = delta * i;
                double high = i == NUM_SEGMENTS - 1 ? 1.0 : delta * (i + 1);

                newRanges.put(low, high);
            }
        }
        return newRanges;
    }
    
    @Test
    public void largeNumScalesScalability() {
        testState = new TestState(false);

        ControllerImpl controller = getController();

        List<List<Segment>> listOfEpochs = scale(controller);
        truncation(controller, listOfEpochs);
        sealAndDeleteStream(controller);
    }
}
