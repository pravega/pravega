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

import io.pravega.client.ClientConfig;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.state.Revision;
import io.pravega.client.state.RevisionedStreamClient;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.client.stream.impl.WriterPosition;
import io.pravega.client.watermark.WatermarkSerializer;
import io.pravega.common.Exceptions;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.IndexAppendProcessor;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.shared.NameUtils;
import io.pravega.shared.watermarks.Watermark;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.utils.ControllerWrapper;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;
import lombok.Cleanup;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Collection of tests to validate controller bootstrap sequence.
 */
public class ControllerWatermarkingTest {

    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final int servicePort = TestUtils.getAvailableListenPort();

    private TestingServer zkTestServer;
    private ControllerWrapper controllerWrapper;
    private PravegaConnectionListener server;
    private ServiceBuilder serviceBuilder;
    private StreamSegmentStore store;
    private TableStore tableStore;

    @Before
    public void setup() throws Exception {
        final String serviceHost = "localhost";
        final int containerCount = 4;

        // 1. Start ZK
        zkTestServer = new TestingServerStarter().start();

        serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        store = serviceBuilder.createStreamSegmentService();
        tableStore = serviceBuilder.createTableStoreService();

        server = new PravegaConnectionListener(false, servicePort, store, tableStore, serviceBuilder.getLowPriorityExecutor(),
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));
        server.startListening();

        // 2. Start controller
        controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), false,
                controllerPort, serviceHost, servicePort, containerCount);

        controllerWrapper.awaitRunning();
    }

    @After
    public void cleanup() throws Exception {
        if (controllerWrapper != null) {
            controllerWrapper.close();
        }
        if (server != null) {
            server.close();
        }
        if (serviceBuilder != null) {
            serviceBuilder.close();
        }
        if (zkTestServer != null) {
            zkTestServer.close();
        }
    }

    @Test(timeout = 60000)
    public void watermarkTest() throws Exception {
        Controller controller = controllerWrapper.getController();
        String scope = "scope";
        String stream = "stream";
        controller.createScope(scope).join();
        StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build();
        controller.createStream(scope, stream, config).join();
        String markStream = NameUtils.getMarkStreamForStream(stream);
        Stream streamObj = new StreamImpl(scope, stream);
        WriterPosition pos1 = WriterPosition.builder().segments(Collections.singletonMap(new Segment(scope, stream, 0L), 10L)).build();
        WriterPosition pos2 = WriterPosition.builder().segments(Collections.singletonMap(new Segment(scope, stream, 0L), 20L)).build();
        
        controller.noteTimestampFromWriter("1", streamObj, 1L, pos1).join();
        controller.noteTimestampFromWriter("2", streamObj, 2L, pos2).join();
        
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder().build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory);
        @Cleanup
        RevisionedStreamClient<Watermark> reader = clientFactory.createRevisionedStreamClient(markStream,
                                                                                              new WatermarkSerializer(),
                                                                                              SynchronizerConfig.builder().build());
        AssertExtensions.assertEventuallyEquals(true, () -> {
            Iterator<Entry<Revision, Watermark>> watermarks = reader.readFrom(reader.fetchOldestRevision());
            return watermarks.hasNext();
        }, 30000);
        Iterator<Entry<Revision, Watermark>> watermarks = reader.readFrom(reader.fetchOldestRevision());
        Watermark watermark = watermarks.next().getValue();
        assertEquals(watermark.getLowerTimeBound(), 1L);
        assertTrue(watermark.getStreamCut().entrySet().stream().anyMatch(x -> x.getKey().getSegmentId() == 0L && x.getValue() ==  20L));

        controller.sealStream(scope, stream).join();
        controller.deleteStream(scope, stream).join();
        
        AssertExtensions.assertFutureThrows("Mark Stream should not exist", 
                controller.getCurrentSegments(scope, markStream),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException);
    }
}
