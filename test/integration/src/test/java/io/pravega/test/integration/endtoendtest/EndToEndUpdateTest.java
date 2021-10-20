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

import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.TruncatedDataException;
import io.pravega.controller.server.eventProcessor.LocalController;
import io.pravega.test.common.ThreadPooledTestSuite;
import io.pravega.test.integration.PravegaResource;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.junit.ClassRule;
import org.junit.Test;

import static io.pravega.test.common.AssertExtensions.assertFutureThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
public class EndToEndUpdateTest extends ThreadPooledTestSuite {

    @ClassRule
    public static final PravegaResource PRAVEGA = new PravegaResource();

    @Override
    protected int getThreadPoolSize() {
        return 1;
    }
    
    @Test(timeout = 30000)
    public void testUpdateStream() throws InterruptedException, ExecutionException, TruncatedDataException, ReinitializationRequiredException {
        String scope = "scope";
        String streamName = "updateStream";

        LocalController controller = (LocalController) PRAVEGA.getLocalController();
        controller.createScope(scope).join();
        controller.createStream(scope, streamName, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build()).join();

        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 2))
                                                        .build();
        controller.updateStream(scope, streamName, config).get();
        
        // verify that stream is updated and also scaled.
        assertEquals(controller.getCurrentSegments(scope, streamName).join().getNumberOfSegments(), 2);

        config = StreamConfiguration.builder()
                                    .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 1))
                                    .build();
        controller.updateStream(scope, streamName, config).get();
        
        // verify that stream is not scaled as min num of segments is still satisfied
        assertEquals(controller.getCurrentSegments(scope, streamName).join().getNumberOfSegments(), 2);

        config = StreamConfiguration.builder()
                                    .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 3))
                                    .build();
        controller.updateStream(scope, streamName, config).get();
        
        // verify that stream is scaled to have 3 segments
        assertEquals(controller.getCurrentSegments(scope, streamName).join().getNumberOfSegments(), 3);

        config = StreamConfiguration.builder()
                                    .scalingPolicy(ScalingPolicy.fixed(6))
                                    .build();
        controller.updateStream(scope, streamName, config).get();
        
        // verify that stream is scaled to have 6 segments
        assertEquals(controller.getCurrentSegments(scope, streamName).join().getNumberOfSegments(), 6);

        config = StreamConfiguration.builder()
                                    .scalingPolicy(ScalingPolicy.fixed(5))
                                    .build();
        controller.updateStream(scope, streamName, config).get();
        
        // verify that stream is scaled to have 3 segments
        assertEquals(controller.getCurrentSegments(scope, streamName).join().getNumberOfSegments(), 5);

        // Seal Stream.
        assertTrue(controller.sealStream(scope, streamName).get());

        config = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(3))
                .build();

        // Attempt to update a sealed stream should complete exceptionally.
        assertFutureThrows("Should throw UnsupportedOperationException",
                controller.updateStream(scope, streamName, config),
                e -> UnsupportedOperationException.class.isAssignableFrom(e.getClass()));
    }
}
