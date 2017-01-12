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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 *
 * @param <T> The type of the event.
 */
public class FlinkPravegaWriter<T> extends RichSinkFunction<T> implements CheckpointedFunction {

    private transient EventStreamWriter<T> pravegaWriter;

    private final PravegaEventSerializer eventSerializer;
    private final PravegaEventRouter<T> eventRouter;

    private final URI controllerURI;
    private final String scopeName;
    private final String streamName;
    private final AtomicReference<Throwable> writeError = new AtomicReference<>(null);
    private PravegaWriterMode writerMode = PravegaWriterMode.ATLEAST_ONCE;

    public FlinkPravegaWriter(final URI controllerURI, final String scope, final String streamName,
            final SerializationSchema<T> serializationSchema, final PravegaEventRouter<T> router) {
        this(controllerURI,
             scope,
             streamName,
             new PravegaEventSerializer<T>() {
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

    public FlinkPravegaWriter(final URI controllerURI, final String scope, final String streamName,
            final PravegaEventSerializer<T> eventSerializer, final PravegaEventRouter<T> router) {
        this.controllerURI = controllerURI;
        this.scopeName = scope;
        this.streamName = streamName;
        this.eventSerializer = eventSerializer;
        this.eventRouter = router;
    }

    public void setPravegaWriterMode(PravegaWriterMode writerMode) {
        this.writerMode = writerMode;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        if (writerMode == PravegaWriterMode.ATLEAST_ONCE) {
            pravegaWriter.flush();
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ClientFactory clientFactory = ClientFactory.withScope(scopeName, controllerURI);
        pravegaWriter = clientFactory.createEventWriter(streamName, eventSerializer, new EventWriterConfig(null));
    }

    @Override
    public void close() throws Exception {
        pravegaWriter.flush();
        pravegaWriter.close();
    }

    @Override
    public void invoke(T event) throws Exception {
        pravegaWriter.writeEvent(eventRouter.getRoutingKey(event), event)
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        writeError.set(throwable);
                    }
                });
    }
}
