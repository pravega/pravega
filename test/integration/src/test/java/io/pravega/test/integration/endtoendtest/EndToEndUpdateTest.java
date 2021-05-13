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
import com.google.common.collect.Lists;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.security.auth.DelegationTokenProviderFactory;
import io.pravega.client.segment.impl.NoSuchSegmentException;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentMetadataClient;
import io.pravega.client.segment.impl.SegmentMetadataClientFactory;
import io.pravega.client.segment.impl.SegmentMetadataClientFactoryImpl;
import io.pravega.client.stream.Checkpoint;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.InvalidStreamException;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.TruncatedDataException;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.server.eventProcessor.LocalController;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import io.pravega.test.integration.PravegaResource;
import io.pravega.test.integration.ReadWriteUtils;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.pravega.shared.NameUtils.computeSegmentId;
import static io.pravega.test.common.AssertExtensions.assertFutureThrows;
import static io.pravega.test.common.AssertExtensions.assertThrows;
import static io.pravega.test.integration.ReadWriteUtils.readEvents;
import static io.pravega.test.integration.ReadWriteUtils.writeEvents;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.*;

@Slf4j
public class EndToEndUpdateTest extends ThreadPooledTestSuite {

    @ClassRule
    public static final PravegaResource PRAVEGA = new PravegaResource();

    @Override
    protected int getThreadPoolSize() {
        return 1;
    }
    
    @Test(timeout = 30000)
    public void testUpdateStream() throws InterruptedException, ExecutionException, TimeoutException,
                                        TruncatedDataException, ReinitializationRequiredException {
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
    }
}
