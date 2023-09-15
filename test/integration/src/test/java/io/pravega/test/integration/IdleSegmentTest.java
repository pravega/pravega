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
package io.pravega.test.integration;

import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ByteBufferSerializer;
import io.pravega.client.stream.mock.MockClientFactory;
import io.pravega.client.stream.mock.MockStreamManager;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.IndexAppendProcessor;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.TestUtils;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class IdleSegmentTest {

    private ServiceBuilder serviceBuilder;

    @Before
    public void setup() throws Exception {
        InternalLoggerFactory.setDefaultFactory(Slf4JLoggerFactory.INSTANCE);
        this.serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        this.serviceBuilder.initialize();
    }

    @After
    public void teardown() {
        this.serviceBuilder.close();
    }

    @Test(timeout = 5000)
    public void testByteBufferEventsWithIdleSegments() throws ReinitializationRequiredException {
        String endpoint = "localhost";
        String streamName = "abc";
        String readerName = "reader";
        String readerGroup = "group";
        int port = TestUtils.getAvailableListenPort();
        ByteBuffer testPayload = ByteBuffer.allocate(100);
        String scope = "Scope1";
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();

        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, tableStore,
                serviceBuilder.getLowPriorityExecutor(), new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));
        server.startListening();
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager(scope, endpoint, port);
        @Cleanup
        MockClientFactory clientFactory = streamManager.getClientFactory();
        ReaderGroupConfig groupConfig = ReaderGroupConfig.builder()
                                                         .stream(Stream.of(scope, streamName))
                                                         .disableAutomaticCheckpoints()
                                                         .build();
        streamManager.createScope(scope);
        streamManager.createStream(scope, streamName,
                                   StreamConfiguration.builder()
                                                      .scalingPolicy(ScalingPolicy.fixed(20))
                                                      .build());
        streamManager.createReaderGroup(readerGroup, groupConfig);
        Serializer<ByteBuffer> serializer = new ByteBufferSerializer();
        @Cleanup
        EventStreamWriter<ByteBuffer> producer = clientFactory.createEventWriter(streamName, serializer,
                                                                             EventWriterConfig.builder().build());
        List<CompletableFuture<Void>> results = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            results.add(producer.writeEvent("FixedRoutingKey", testPayload));
            System.out.println("Writing event " + i);
        }
        producer.flush();

        @Cleanup
        EventStreamReader<ByteBuffer> reader = clientFactory.createReader(readerName, readerGroup, serializer,
                                                                      ReaderConfig.builder().build());
        for (int i = 0; i < 10; i++) {
            ByteBuffer read = reader.readNextEvent(10000).getEvent();
            assertEquals(testPayload, read);
        }
    }

}
