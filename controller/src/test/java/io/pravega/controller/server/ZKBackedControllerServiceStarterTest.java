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
package io.pravega.controller.server;

import com.google.common.collect.Lists;
import com.google.common.primitives.Bytes;
import com.google.common.util.concurrent.Service;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pravega.client.connection.impl.ClientConnection;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.function.Callbacks;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.eventProcessor.impl.ControllerEventProcessorConfigImpl;
import io.pravega.controller.server.impl.ControllerServiceConfigImpl;
import io.pravega.controller.server.rpc.grpc.impl.GRPCServerConfigImpl;
import io.pravega.controller.store.client.StoreClient;
import io.pravega.controller.store.client.StoreClientConfig;
import io.pravega.controller.store.client.StoreClientFactory;
import io.pravega.controller.store.client.ZKClientConfig;
import io.pravega.controller.store.client.impl.ZKClientConfigImpl;
import io.pravega.controller.store.host.HostMonitorConfig;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.store.kvtable.KVTableMetadataStore;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.timeout.TimeoutServiceConfig;
import io.pravega.controller.util.Config;
import io.pravega.shared.protocol.netty.Append;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.SecurityConfigDefaults;
import io.pravega.test.common.TestingServerStarter;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Cleanup;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

/**
 * ControllerServiceStarter backed by ZK store tests.
 */
@Slf4j
public abstract class ZKBackedControllerServiceStarterTest extends ControllerServiceStarterTest {
    private TestingServer zkServer;
    private ConcurrentHashMap<String, byte[]> segments;
    private ConcurrentHashMap<String, List<WireCommands.Event>> segmentEvent;
    private ConcurrentHashMap<UUID, String> writers;
    private ConcurrentHashMap<UUID, Long> writerEventNumber;

    public ZKBackedControllerServiceStarterTest() {
        super(true, false);
    }

    @Override
    public void setup() throws Exception {
        zkServer = new TestingServerStarter().start();

        ZKClientConfig zkClientConfig = ZKClientConfigImpl.builder().connectionString(zkServer.getConnectString())
                .initialSleepInterval(500)
                .maxRetries(10)
                .namespace("pravega/" + UUID.randomUUID())
                .sessionTimeoutMs(10 * 1000)
                .build();
        storeClientConfig = getStoreConfig(zkClientConfig);
        storeClient = StoreClientFactory.createStoreClient(storeClientConfig);
        executor = ExecutorServiceHelpers.newScheduledThreadPool(5, "test");
        segments = new ConcurrentHashMap<>();
        segmentEvent = new ConcurrentHashMap<>();
        writers = new ConcurrentHashMap<>();
        writerEventNumber = new ConcurrentHashMap<>();
    }
    
    @Override
    public void tearDown() throws Exception {
        storeClient.close();
        zkServer.close();
        ExecutorServiceHelpers.shutdown(executor);
    }

    abstract StoreClientConfig getStoreConfig(ZKClientConfig zkClientConfig);
    
    abstract StreamMetadataStore getStore(StoreClient storeClient);

    abstract KVTableMetadataStore getKVTStore(StoreClient storeClient);

    private class MockConnectionFactory implements ConnectionFactory {
        @Getter
        private ReplyProcessor rp;
        private ClientConnection connection;
        private final ScheduledExecutorService executorService = ExecutorServiceHelpers.newScheduledThreadPool(2, "test");

        @Override
        public CompletableFuture<ClientConnection> establishConnection(PravegaNodeUri endpoint, ReplyProcessor rp) {
            this.rp = rp;
            this.connection = new MockConnection(rp);
            return CompletableFuture.completedFuture(connection);
        }

        @Override
        public ScheduledExecutorService getInternalExecutor() {
            return executorService;
        }

        @Override
        public void close() {
            if (connection != null) {
                connection.close();
            }
            ExecutorServiceHelpers.shutdown(executorService);
        }
    }

    private class MockConnection implements ClientConnection {
        @Getter
        private final ReplyProcessor rp;

        public MockConnection(ReplyProcessor rp) {
            this.rp = rp;
        }

        @Override
        public void send(WireCommand cmd) throws ConnectionFailedException {
            handleRequest(cmd);
        }
        
        @Override
        public void send(Append append) throws ConnectionFailedException {
        }

        @Override
        public void sendAsync(List<Append> appends, CompletedCallback callback) {
        }

        @Override
        public void close() {

        }

        @Override
        public PravegaNodeUri getLocation() {
            return null;
        }

        private void handleRequest(WireCommand cmd) {
            switch (cmd.getType()) {
                case GET_STREAM_SEGMENT_INFO:
                    WireCommands.GetStreamSegmentInfo getStreamSegmentInfo = (WireCommands.GetStreamSegmentInfo) cmd;
                    long writeOffset = segments.getOrDefault(getStreamSegmentInfo.getSegmentName(), new byte[0]).length;
                    rp.process(new WireCommands.StreamSegmentInfo(getStreamSegmentInfo.getRequestId(),
                            getStreamSegmentInfo.getSegmentName(), true, false, false, System.currentTimeMillis(),
                            writeOffset, 0L));

                    break;
                case SETUP_APPEND:
                    WireCommands.SetupAppend setupAppend = (WireCommands.SetupAppend) cmd;
                    long lastEventNumber = writerEventNumber.getOrDefault(setupAppend.getWriterId(), 0L);
                    writers.put(setupAppend.getWriterId(), setupAppend.getSegment());
                    segments.putIfAbsent(setupAppend.getSegment(), new byte[0]);
                    rp.process(new WireCommands.AppendSetup(setupAppend.getRequestId(), setupAppend.getSegment(), setupAppend.getWriterId(),
                            lastEventNumber));
                    break;
                case CONDITIONAL_APPEND:
                    WireCommands.ConditionalAppend conditionalAppend = (WireCommands.ConditionalAppend) cmd;
                    String segment = writers.getOrDefault(conditionalAppend.getWriterId(), "");
                    long segmentOffset = segments.getOrDefault(segment, new byte[0]).length;
                    if (segmentOffset == conditionalAppend.getExpectedOffset()) {
                        segments.compute(segment, (x, y) -> {
                            byte[] toWrite = conditionalAppend.getEvent().getAsByteBuf().nioBuffer().array();
                            if (y != null) {
                                byte[] ret = new byte[y.length + toWrite.length];
                                return Bytes.concat(y, toWrite);
                            } else {
                                return toWrite;
                            }
                        });
                        segmentEvent.compute(segment, (x, y) -> {
                            if (y != null) {
                                y.add(conditionalAppend.getEvent());
                                return y;
                            } else {
                                return Lists.newArrayList(conditionalAppend.getEvent());
                            }
                        });
                        rp.process(new WireCommands.DataAppended(conditionalAppend.getRequestId(), conditionalAppend.getWriterId(),
                                conditionalAppend.getEventNumber(), 0L, 0L));
                    }
                    break;

                case READ_SEGMENT:
                    WireCommands.ReadSegment readSegment = (WireCommands.ReadSegment) cmd;
                    if (segments.containsKey(readSegment.getSegment())) {
                        byte[] array = segments.get(readSegment.getSegment());
                        int offset = (int) readSegment.getOffset();
                        if (array.length > offset) {
                            ByteBuf buff = Unpooled.wrappedBuffer(array, offset, array.length - offset);
                            rp.process(new WireCommands.SegmentRead(readSegment.getSegment(), offset, true,
                                    false, buff, readSegment.getRequestId()));
                        }
                    } else {
                        ByteBuf buff = Unpooled.wrappedBuffer(new byte[0]);
                        rp.process(new WireCommands.SegmentRead(readSegment.getSegment(), 0L, true,
                                false, buff, readSegment.getRequestId()));
                    }
                    break;
                case GET_SEGMENT_ATTRIBUTE:
                    WireCommands.GetSegmentAttribute getSegmentAttribute = (WireCommands.GetSegmentAttribute) cmd;
                    rp.process(new WireCommands.SegmentAttribute(getSegmentAttribute.getRequestId(), 0L));
                    break;
                default:
                    rp.processingFailure(new RuntimeException("unexpected"));
            }
        }
    }

    protected ControllerServiceConfig createControllerServiceConfigWithEventProcessors() {
        HostMonitorConfig hostMonitorConfig = HostMonitorConfigImpl.builder()
                                                                   .hostMonitorEnabled(false)
                                                                   .hostMonitorMinRebalanceInterval(Config.CLUSTER_MIN_REBALANCE_INTERVAL)
                                                                   .containerCount(Config.HOST_STORE_CONTAINER_COUNT)
                                                                   .hostContainerMap(HostMonitorConfigImpl.getHostContainerMap(Config.SERVICE_HOST, Config.SERVICE_PORT,
                                                                           Config.HOST_STORE_CONTAINER_COUNT))
                                                                   .build();

        TimeoutServiceConfig timeoutServiceConfig = TimeoutServiceConfig.builder()
                                                                        .maxLeaseValue(Config.MAX_LEASE_VALUE)
                                                                        .build();

        return ControllerServiceConfigImpl.builder()
                                          .threadPoolSize(15)
                                          .storeClientConfig(storeClientConfig)
                                          .controllerClusterListenerEnabled(false)
                                          .hostMonitorConfig(hostMonitorConfig)
                                          .timeoutServiceConfig(timeoutServiceConfig)
                                          .eventProcessorConfig(Optional.of(ControllerEventProcessorConfigImpl.withDefault()))
                                          .grpcServerConfig(Optional.of(GRPCServerConfigImpl.builder()
                                                                                            .port(grpcPort)
                                                                                            .authorizationEnabled(false)
                                                                                            .tlsEnabled(false)
                                                                                            .tlsProtocolVersion(SecurityConfigDefaults.TLS_PROTOCOL_VERSION)
                                                                                            .build()))
                                          .restServerConfig(Optional.empty())
                                          .minBucketRedistributionIntervalInSeconds(10)
                                          .build();
    }

    @Test(timeout = 30000L)
    public void testStallEventProcStartupWithSessionExpiration() throws Exception {
        // mock createStream call to throw an exception so that we can keep failing create stream and retrying indefinitely
        // controller event processor will be stuck at this point. 
        // now expire the session. basically call notify on starter
        CompletableFuture<Void> latch = new CompletableFuture<>();
        CompletableFuture<Void> signal = new CompletableFuture<>();
        @Cleanup
        StreamMetadataStore store = spy(getStore(storeClient));
        KVTableMetadataStore kvtStore = spy(getKVTStore(storeClient));
        doAnswer(x -> {
            signal.complete(null);
            latch.join();
            throw new RuntimeException();
        }).when(store).createScope(anyString(), any(), any());

        ControllerServiceStarter starter = new ControllerServiceStarter(createControllerServiceConfigWithEventProcessors(), storeClient,
                SegmentHelperMock.getSegmentHelperMockForTables(executor), new MockConnectionFactory(), store, kvtStore);
        starter.startAsync();
        
        // this will block until createScope is called from event processors. 
        signal.join();
        latch.complete(null);
        starter.notifySessionExpiration();

        AssertExtensions.assertThrows(IllegalStateException.class, starter::awaitRunning);
        assertTrue(Exceptions.unwrap(starter.failureCause()) instanceof ZkSessionExpirationException);
        assertEquals(starter.state(), Service.State.FAILED);
        Callbacks.invokeSafely(starter::shutDown, null);
    }
}
