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
package io.pravega.controller.server.health;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Service;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.common.cluster.Host;
import io.pravega.controller.eventProcessor.EventProcessorGroup;
import io.pravega.controller.eventProcessor.EventProcessorSystem;
import io.pravega.controller.eventProcessor.impl.EventProcessorSystemImpl;
import io.pravega.controller.server.eventProcessor.ControllerEventProcessorConfig;
import io.pravega.controller.server.eventProcessor.ControllerEventProcessors;
import io.pravega.controller.server.eventProcessor.LocalController;
import io.pravega.controller.server.eventProcessor.impl.ControllerEventProcessorConfigImpl;
import io.pravega.controller.store.checkpoint.CheckpointStore;
import io.pravega.controller.store.checkpoint.CheckpointStoreException;
import io.pravega.controller.store.kvtable.KVTableMetadataStore;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.task.KeyValueTable.TableMetadataTasks;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.shared.health.Health;
import io.pravega.shared.health.Status;
import io.pravega.test.common.ThreadPooledTestSuite;

import lombok.SneakyThrows;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Unit tests for EventProcessorHealthContributor
 */
public class EventProcessorHealthContributorTest extends ThreadPooledTestSuite {
    private ControllerEventProcessors eventProcessors;
    private EventProcessorHealthContributor contributor;
    private Health.HealthBuilder builder;

    @SneakyThrows
    @Before
    public void setup() {
        Host host = mock(Host.class);
        LocalController localController = mock(LocalController.class);
        CheckpointStore checkpointStore = mock(CheckpointStore.class);
        StreamMetadataStore streamStore = mock(StreamMetadataStore.class);
        BucketStore bucketStore = mock(BucketStore.class);
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        StreamMetadataTasks streamMetadataTasks = mock(StreamMetadataTasks.class);
        StreamTransactionMetadataTasks streamTransactionMetadataTasks = mock(StreamTransactionMetadataTasks.class);
        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        KVTableMetadataStore kvtMetadataStore = mock(KVTableMetadataStore.class);
        TableMetadataTasks kvtMetadataTasks = mock(TableMetadataTasks.class);
        EventProcessorSystem system = mock(EventProcessorSystemImpl.class);
        ControllerEventProcessorConfig config = ControllerEventProcessorConfigImpl.withDefault();

        EventProcessorGroup<ControllerEvent> processor = getProcessor();
        EventProcessorGroup<ControllerEvent> mockProcessor = spy(processor);
        doThrow(new CheckpointStoreException("host not found")).when(mockProcessor).notifyProcessFailure("host3");
        when(system.createEventProcessorGroup(any(), any(), any())).thenReturn(mockProcessor);

        eventProcessors = spy(new ControllerEventProcessors(host.getHostId(),
                config, localController, checkpointStore, streamStore,
                bucketStore, connectionPool, streamMetadataTasks, streamTransactionMetadataTasks, kvtMetadataStore,
                kvtMetadataTasks, system, executorService()));
        doReturn(true).when(eventProcessors).isReady();

        contributor = new EventProcessorHealthContributor("eventprocessors", eventProcessors);
        builder = Health.builder().name("eventprocessors");
    }

    @After
    public void tearDown() {
        contributor.close();
        eventProcessors.close();
    }

    @Test
    public void testHealthCheck() throws Exception {
        eventProcessors.startAsync();
        eventProcessors.awaitRunning();
        Status status = contributor.doHealthCheck(builder);
        Assert.assertEquals(Status.UP, status);
        eventProcessors.stopAsync();
        eventProcessors.awaitTerminated();
        status = contributor.doHealthCheck(builder);
        Assert.assertEquals(Status.DOWN, status);
    }

    private EventProcessorGroup<ControllerEvent> getProcessor() {
        return new EventProcessorGroup<ControllerEvent>() {
            @Override
            public void notifyProcessFailure(String process) throws CheckpointStoreException {

            }

            @Override

            public EventStreamWriter<ControllerEvent> getWriter() {
                return null;
            }

            @Override
            public Set<String> getProcesses() throws CheckpointStoreException {
                return Sets.newHashSet("host1", "host2");
            }

            @Override
            @SuppressFBWarnings("NP_NONNULL_RETURN_VIOLATION")
            public Service startAsync() {
                return null;
            }

            @Override
            public boolean isRunning() {
                return false;
            }

            @Override
            @SuppressFBWarnings("NP_NONNULL_RETURN_VIOLATION")
            public State state() {
                return null;
            }

            @Override
            @SuppressFBWarnings("NP_NONNULL_RETURN_VIOLATION")
            public Service stopAsync() {
                return null;
            }

            @Override
            public void awaitRunning() {

            }

            @Override
            public void awaitRunning(long timeout, TimeUnit unit) throws TimeoutException {

            }

            @Override
            public void awaitTerminated() {

            }

            @Override
            public void awaitTerminated(long timeout, TimeUnit unit) throws TimeoutException {

            }

            @Override
            @SuppressFBWarnings("NP_NONNULL_RETURN_VIOLATION")
            public Throwable failureCause() {
                return null;
            }

            @Override
            public void addListener(Listener listener, Executor executor) {

            }

            @Override
            public void close() throws Exception {

            }
        };
    }
}
