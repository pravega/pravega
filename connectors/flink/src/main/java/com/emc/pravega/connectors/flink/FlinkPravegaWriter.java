/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.connectors.flink;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.Serializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.hadoop.shaded.com.google.common.base.Preconditions;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

import java.io.Serializable;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Flink sink implementation for writing into pravega storage.
 *
 * @param <T> The type of the event to be written.
 */
@Slf4j
public class FlinkPravegaWriter<T> extends RichSinkFunction<T> implements CheckpointedFunction, Serializable {
    // The event serializer for pravega.
    private final Serializer eventSerializer;

    // The router used to partition events within a stream.
    private final PravegaEventRouter<T> eventRouter;

    // The pravega controller endpoint.
    private final URI controllerURI;

    // The scope name of the destination stream.
    private final String scopeName;

    // The pravega stream name to write events to.
    private final String streamName;

    // The sink's mode of operation. This is used to provide different guarantees for the written events.
    private PravegaWriterMode writerMode = PravegaWriterMode.ATLEAST_ONCE;

    // Following runtime parameters are populated when the flink sub-tasks are executed at the task managers.

    // The pravega writer client.
    private transient EventStreamWriter<T> pravegaWriter = null;

    // Error which will be detected asynchronously and reported to flink.
    private transient AtomicReference<Throwable> writeError = null;

    // Used to track confirmation from all writes to ensure guaranteed writes.
    private transient AtomicInteger pendingWritesCount = null;

    /**
     * The flink pravega writer instance which can be added as a sink to a flink job.
     *
     * @param controllerURI         The pravega controller endpoint address.
     * @param scope                 The destination stream's scope name.
     * @param streamName            The destination stream Name.
     * @param serializationSchema   The implementation for serializing every event into pravega's storage format.
     * @param router                The implementation to extract the partition key from the event.
     */
    public FlinkPravegaWriter(final URI controllerURI, final String scope, final String streamName,
            final SerializationSchema<T> serializationSchema, final PravegaEventRouter<T> router) {
        this(controllerURI,
             scope,
             streamName,
             new Serializer<T>() {
                    @Override
                    public ByteBuffer serialize(T event) {
                        return ByteBuffer.wrap(serializationSchema.serialize(event));
                    }

                    @Override
                    public T deserialize(ByteBuffer serializedValue) {
                        return null;
                    }
                },
             router);
    }

    private FlinkPravegaWriter(final URI controllerURI, final String scope, final String streamName,
            final Serializer<T> eventSerializer, final PravegaEventRouter<T> router) {
        Preconditions.checkNotNull(controllerURI);
        Preconditions.checkNotNull(scope);
        Preconditions.checkNotNull(streamName);
        Preconditions.checkNotNull(eventSerializer);
        Preconditions.checkNotNull(router);

        this.controllerURI = controllerURI;
        this.scopeName = scope;
        this.streamName = streamName;
        this.eventSerializer = eventSerializer;
        this.eventRouter = router;
    }

    /**
     * Set this writer's operating mode.
     *
     * @param writerMode    The mode of operation.
     */
    public void setPravegaWriterMode(PravegaWriterMode writerMode) {
        this.writerMode = writerMode;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.writeError = new AtomicReference<>(null);
        this.pendingWritesCount = new AtomicInteger(0);
        ClientFactory clientFactory = ClientFactory.withScope(this.scopeName, this.controllerURI);
        this.pravegaWriter = clientFactory.createEventWriter(
                this.streamName,
                this.eventSerializer,
                new EventWriterConfig(null));
        log.info("Initialized pravega writer for stream: {}/{} with controller URI: {}", this.scopeName, this.streamName,
                 this.controllerURI);
    }

    @Override
    public void close() throws Exception {
        if (this.writerMode == PravegaWriterMode.ATLEAST_ONCE) {
            flushAndVerify();
        }
        this.pravegaWriter.close();
    }

    @Override
    public void invoke(T event) throws Exception {
        if (this.writerMode == PravegaWriterMode.ATLEAST_ONCE) {
            Throwable error = this.writeError.getAndSet(null);
            if (error != null) {
                throw new Exception(error);
            }
        }

        this.pravegaWriter.writeEvent(this.eventRouter.getRoutingKey(event), event)
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        log.warn("Detected a write failure: ", throwable);

                        // We will record only the first error detected, since this will mostly likely help with
                        // finding the root cause. Storing all errors will not be feasible.
                        this.writeError.compareAndSet(null, throwable);
                    }
                    if (this.writerMode == PravegaWriterMode.ATLEAST_ONCE) {
                        synchronized (this.pendingWritesCount) {
                            this.pendingWritesCount.decrementAndGet();
                            this.pendingWritesCount.notify();
                        }
                    }
                });
        this.pendingWritesCount.incrementAndGet();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        if (this.writerMode == PravegaWriterMode.ATLEAST_ONCE) {
            log.debug("Snapshot triggered, wait for all pending writes to complete");
            flushAndVerify();
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // Nothing to restore.
    }

    // Wait until all pending writes are completed and throw any errors detected.
    private void flushAndVerify() throws Exception {
        this.pravegaWriter.flush();

        // Wait until all errors, if any, have been recorded.
        synchronized (this.pendingWritesCount) {
            while (this.pendingWritesCount.get() > 0) {
                this.pendingWritesCount.wait();
            }
        }

        // Verify that no events have been lost so far.
        Throwable error = this.writeError.getAndSet(null);
        if (error != null) {
            log.error("Failure detected: " + error);
            throw new Exception(error);
        }
    }
}
