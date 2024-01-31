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
package io.pravega.client.admin.impl;


import io.pravega.client.ClientConfig;
import io.pravega.client.connection.impl.ClientConnection;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.SegmentReader;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import lombok.Cleanup;
import org.junit.Test;
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Test class for SegmentReaderManager
 */
public class SegmentReaderManagerImplTest {

    private JavaSerializer<String> serializer = new JavaSerializer<>();
    @Test(timeout = 10000)
    public void testGetSegmentReaderswithStreamCut() {

        String scopeName = "scope_getSegmentReaders";
        String streamName = "stream_getSegmentReaders";
        PravegaNodeUri location = new PravegaNodeUri("localhost", 0);
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = getMockConnectionFactory(location);
        MockController mockController = new MockController(location.getEndpoint(), location.getPort(), connectionFactory, false);
        Stream stream = createStream(scopeName, streamName, 3, mockController);
        @Cleanup
        SegmentReaderManagerImpl<String> segmentReaderManager =
                new SegmentReaderManagerImpl<>(mockController, ClientConfig.builder().maxConnectionsPerSegmentStore(1).build(), connectionFactory, serializer);
        Segment segment1 = new Segment(scopeName, streamName, 1L);
        Segment segment2 = new Segment(scopeName, streamName, 2L);
        Segment segment3 = new Segment(scopeName, streamName, 3L);
        Map<Segment, Long> positionMap = new HashMap<>();
        positionMap.put(segment1, 20L);
        positionMap.put(segment2, 30L);
        positionMap.put(segment3, 50L);
        StreamCut startingSC = new StreamCutImpl(stream, positionMap);
        List<SegmentReader<String>> segmentReaderManagerList = segmentReaderManager.getSegmentReaders(stream, startingSC).join();
        assertEquals(3, segmentReaderManagerList.size());

        Stream stream2 = createStream("scope2", "stream2", 2, mockController);
        List<SegmentReader<String>> segmentReaderManagerList2 = segmentReaderManager.getSegmentReaders(stream2, StreamCut.UNBOUNDED).join();
        assertEquals(2, segmentReaderManagerList2.size());
    }

    private Stream createStream(String scope, String streamName, int numSegments, MockController mockController) {
        Stream stream = new StreamImpl(scope, streamName);
        mockController.createScope(scope);
        mockController.createStream(scope, streamName, StreamConfiguration.builder()
                                                       .scalingPolicy(ScalingPolicy.fixed(numSegments))
                                                       .build())
                      .join();
        return stream;
    }

    private MockConnectionFactoryImpl getMockConnectionFactory(PravegaNodeUri location) {
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        ClientConnection connection = mock(ClientConnection.class);
        connectionFactory.provideConnection(location, connection);
        return connectionFactory;
    }

}
