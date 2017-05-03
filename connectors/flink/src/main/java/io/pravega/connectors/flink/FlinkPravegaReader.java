/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
package io.pravega.connectors.flink;

import io.pravega.ClientFactory;
import io.pravega.ReaderGroupManager;
import io.pravega.stream.EventRead;
import io.pravega.stream.EventStreamReader;
import io.pravega.stream.ReaderConfig;
import io.pravega.stream.ReaderGroupConfig;
import io.pravega.stream.Serializer;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.functions.StoppableFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;

/**
 * Flink source implementation for reading from pravega storage.
 *
 * @param <T> The type of the event to be written.
 */
@Slf4j
public class FlinkPravegaReader<T> extends RichParallelSourceFunction<T> implements ResultTypeQueryable<T>,
        StoppableFunction, Serializable {
    private static final long serialVersionUID = 1L;

    // The supplied event deserializer.
    private final DeserializationSchema<T> deserializationSchema;

    // The pravega controller endpoint.
    private final URI controllerURI;

    // The scope name of the destination stream.
    private final String scopeName;

    // The readergroup name to coordinate the parallel readers. This should be unique for a flink job.
    private final String readerGroupName;

    // Following runtime parameters are populated when the flink sub-tasks are executed at the task managers.

    // The pravega reader instance for each flink's sub-task.
    private transient EventStreamReader<T> pravegaReader = null;

    // The reader Id of the reader created for this subtask.
    private transient String readerId = null;

    // Flag to terminate the source.
    private transient AtomicBoolean isRunning = null;

    /**
     * The flink pravega reader instance which can be added as a source to a flink job.
     *
     * @param controllerURI         The pravega controller endpoint address.
     * @param scope                 The destination stream's scope name.
     * @param streamNames           The list of stream names to read events from.
     * @param startTime             The start time from when to read events from.
     *                              Use 0 to read all stream events from the beginning.
     * @param deserializationSchema The implementation to deserialize events from pravega streams.
     */
    public FlinkPravegaReader(final URI controllerURI, final String scope, final Set<String> streamNames,
            final long startTime, final DeserializationSchema<T> deserializationSchema) {
        Preconditions.checkNotNull(controllerURI);
        Preconditions.checkNotNull(scope);
        Preconditions.checkNotNull(streamNames);
        Preconditions.checkArgument(startTime >= 0);
        Preconditions.checkNotNull(deserializationSchema);

        this.controllerURI = controllerURI;
        this.scopeName = scope;
        this.deserializationSchema = deserializationSchema;
        this.readerGroupName = "flink" + RandomStringUtils.randomAlphanumeric(20).toLowerCase();

        // TODO: This will require the client to have access to the pravega controller and handle any temporary errors.
        //       See https://github.com/pravega/pravega/issues/553.
        log.info("Creating reader group: {} for the flink job", this.readerGroupName);

        ReaderGroupManager.withScope(scope, controllerURI)
                .createReaderGroup(this.readerGroupName, ReaderGroupConfig.builder().startingTime(startTime).build(),
                                   streamNames);
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        log.info("Starting pravega reader, ID: " + this.readerId);
        while (this.isRunning.get()) {
            EventRead<T> eventRead = this.pravegaReader.readNextEvent(1000);
            if (eventRead.getEvent() != null) {
                if (this.deserializationSchema.isEndOfStream(eventRead.getEvent())) {
                    // Found stream end marker.
                    // TODO: Handle scenario when reading from multiple segments. This will be cleaned up as part of:
                    //       https://github.com/pravega/pravega/issues/551.
                    log.info("Reached end of stream for reader: {}", this.readerId);
                    return;
                }
                ctx.collect(eventRead.getEvent());
            }
        }
    }

    @Override
    public void cancel() {
        this.isRunning.set(false);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.isRunning = new AtomicBoolean(true);
        final Serializer<T> deserializer = new Serializer<T>() {
            @Override
            public ByteBuffer serialize(final T event) {
                throw new IllegalStateException("serialize() called within a deserializer");
            }

            @Override
            public T deserialize(final ByteBuffer serializedValue) {
                try {
                    return deserializationSchema.deserialize(serializedValue.array());
                } catch (IOException e) {
                    // Converting exception since the base method doesn't handle checked exceptions.
                    throw new RuntimeException(e);
                }
            }
        };
        this.readerId = "flink-reader-" + UUID.randomUUID();
        this.pravegaReader = ClientFactory.withScope(this.scopeName, this.controllerURI)
                .createReader(this.readerId, this.readerGroupName, deserializer, ReaderConfig.builder().build());

        log.info("Initialized pravega reader with controller URI: {}", this.controllerURI);
    }

    @Override
    public void close() throws Exception {
        if (this.pravegaReader != null) {
            this.pravegaReader.close();
        }
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return this.deserializationSchema.getProducedType();
    }

    @Override
    public void stop() {
        this.isRunning.set(false);
    }
}
