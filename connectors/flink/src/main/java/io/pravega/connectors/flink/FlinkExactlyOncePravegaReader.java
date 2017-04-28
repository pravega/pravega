/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.connectors.flink;

import com.google.common.base.Preconditions;

import io.pravega.ClientFactory;
import io.pravega.ReaderGroupManager;
import io.pravega.stream.Checkpoint;
import io.pravega.stream.EventRead;
import io.pravega.stream.EventStreamReader;
import io.pravega.stream.ReaderConfig;
import io.pravega.stream.ReaderGroupConfig;
import io.pravega.stream.Serializer;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.RandomStringUtils;

import org.apache.flink.api.common.functions.StoppableFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.streaming.api.checkpoint.ExternallyInducedSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.util.FlinkException;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.UUID;

/**
 * Flink source implementation for reading from pravega storage.
 *
 * @param <T> The type of the event to be written.
 */
@Slf4j
public class FlinkExactlyOncePravegaReader<T>
        extends RichParallelSourceFunction<T> 
        implements ResultTypeQueryable<T>, StoppableFunction, ExternallyInducedSource<T, Checkpoint> {

    private static final long serialVersionUID = 1L;

    // The supplied event deserializer.
    private final DeserializationSchema<T> deserializationSchema;

    // The pravega controller endpoint.
    private final URI controllerURI;

    // The scope name of the destination stream.
    private final String scopeName;

    // The readergroup name to coordinate the parallel readers. This should be unique for a Flink job.
    private final String readerGroupName;
    
    // the name of the reader, used to store state and resume existing state from savepoints
    private final String readerName;

    // Flag to terminate the source. volatile, because 'stop()' and 'cancel()' 
    // may be called asynchronously 
    private volatile boolean running = true;

    // checkpoint trigger callback, invoked when a checkpoint event is received.
    // no need to be volatile, the source is driven by but one thread
    private transient CheckpointTrigger checkpointTrigger;

    // ------------------------------------------------------------------------

    /**
     * Creates a new Flink Pravega reader instance which can be added as a source to a Flink job.
     * 
     * <p>The reader will use a random name under which it stores its state in a checkpoint. While
     * checkpoints still work, this means that matching the state into another Flink jobs
     * (when resuming from a savepoint) will not be possible. Thus it is generally recommended
     * to give a reader name to each reader.
     *
     * @param controllerURI         The pravega controller endpoint address.
     * @param scope                 The destination stream's scope name.
     * @param streamNames           The list of stream names to read events from.
     * @param startTime             The start time from when to read events from.
     *                              Use 0 to read all stream events from the beginning.
     * @param deserializationSchema The implementation to deserialize events from pravega streams.
     */
    public FlinkExactlyOncePravegaReader(final URI controllerURI, final String scope, final Set<String> streamNames,
                                         final long startTime, final DeserializationSchema<T> deserializationSchema) {

        this(controllerURI, scope, streamNames, startTime, deserializationSchema, UUID.randomUUID().toString());
    }

    /**
     * Creates a new Flink Pravega reader instance which can be added as a source to a Flink job.
     * 
     * <p>The reader will use the given {@code readerName} to store its state (its positions
     * in the stream segments) in Flink's checkpoints/savepoints. This name is used in a similar
     * way as the operator UIDs ({@link SingleOutputStreamOperator#uid(String)}) to identify state
     * when matching it into another job that resumes from this job's checkpoints/savepoints.
     * 
     * <p>Without specifying a {@code readerName}, the job will correctly checkpoint and recover,
     * but new instances of the job can typically not resume this reader's state (positions).
     *
     * @param controllerURI         The pravega controller endpoint address.
     * @param scope                 The destination stream's scope name.
     * @param streamNames           The list of stream names to read events from.
     * @param startTime             The start time from when to read events from.
     *                              Use 0 to read all stream events from the beginning.
     * @param deserializationSchema The implementation to deserialize events from pravega streams.
     * @param readerName            The name of the reader, used to store state and resume existing state from
     *                              savepoints.
     */
    public FlinkExactlyOncePravegaReader(final URI controllerURI, final String scope, final Set<String> streamNames,
                                         final long startTime, final DeserializationSchema<T> deserializationSchema,
                                         final String readerName) {

        Preconditions.checkNotNull(controllerURI);
        Preconditions.checkNotNull(scope);
        Preconditions.checkNotNull(streamNames);
        Preconditions.checkArgument(startTime >= 0);
        Preconditions.checkNotNull(deserializationSchema);
        Preconditions.checkNotNull(readerName);

        this.controllerURI = controllerURI;
        this.scopeName = scope;
        this.deserializationSchema = deserializationSchema;
        this.readerGroupName = "flink" + RandomStringUtils.randomAlphanumeric(20).toLowerCase();
        this.readerName = readerName;

        // TODO: This will require the client to have access to the pravega controller and handle any temporary errors.
        //       See https://github.com/pravega/pravega/issues/553.
        log.info("Creating reader group: {} for the Flink job", this.readerGroupName);

        ReaderGroupManager.withScope(scope, controllerURI)
                .createReaderGroup(this.readerGroupName, ReaderGroupConfig.builder().startingTime(startTime).build(),
                                   streamNames);
    }

    // ------------------------------------------------------------------------
    //  source function methods
    // ------------------------------------------------------------------------

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        // the reader ID is random unique per source task
        final String readerId = "flink-reader-" + UUID.randomUUID();

        log.info("{} : Creating Pravega reader with ID '{}' for controller URI: ",
                getRuntimeContext().getTaskNameWithSubtasks(), readerId, this.controllerURI);

        // create the adapter between Pravega's serializers and Flink's serializers
        final Serializer<T> deserializer = new FlinkDeserializer<>(this.deserializationSchema);
        
        // build the reader
        try (EventStreamReader<T> pravegaReader =  ClientFactory.withScope(this.scopeName, this.controllerURI)
                .createReader(readerId, this.readerGroupName, deserializer, ReaderConfig.builder().build())) {

            log.info("Starting Pravega reader '{}' for controller URI {}: " + readerId, this.controllerURI);

            // main work loop, which this task is running
            while (this.running) {
                final EventRead<T> eventRead = pravegaReader.readNextEvent(1000);
                final T event = eventRead.getEvent();

                // emit the event, if one was carried
                if (event != null) {
                    if (this.deserializationSchema.isEndOfStream(event)) {
                        // Found stream end marker.
                        // TODO: Handle scenario when reading from multiple segments. This will be cleaned up as part of:
                        //       https://github.com/pravega/pravega/issues/551.
                        log.info("Reached end of stream for reader: {}", readerId);
                        return;
                    }
                    ctx.collect(event);
                }

                // if the read marks a checkpoint, trigger the checkpoint
                if (eventRead.isCheckpoint()) {
                    triggerCheckpoint(eventRead.getCheckpointName());
                }
            }
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }

    @Override
    public void stop() {
        this.running = false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return this.deserializationSchema.getProducedType();
    }

    // ------------------------------------------------------------------------
    //  checkpoints
    // ------------------------------------------------------------------------

    @Override
    public MasterTriggerRestoreHook<Checkpoint> createMasterTriggerRestoreHook() {
        return new ReaderCheckpointHook(this.readerName, this.readerGroupName, this.scopeName, this.controllerURI);
    }

    @Override
    public void setCheckpointTrigger(CheckpointTrigger checkpointTrigger) {
        this.checkpointTrigger = checkpointTrigger;
    }

    /**
     * Triggers the checkpoint in the Flink source operator.
     *
     * <p>This method assumes that the {@code checkpointIdentifier} is a string of the form
     */
    private void triggerCheckpoint(String checkpointIdentifier) throws FlinkException {
        Preconditions.checkState(checkpointTrigger != null, "checkpoint trigger not set");

        final long checkpointId;
        try {
            checkpointId = ReaderCheckpointHook.parseCheckpointId(checkpointIdentifier);
        } catch (IllegalArgumentException e) {
            throw new FlinkException("Cannot trigger checkpoint due to invalid Pravega checkpoint name", e.getCause());
        }

        checkpointTrigger.triggerCheckpoint(checkpointId);
    }

    // ------------------------------------------------------------------------
    //  serializer
    // ------------------------------------------------------------------------

    private static final class FlinkDeserializer<T> implements Serializer<T> {

        private final DeserializationSchema<T> deserializationSchema;

        FlinkDeserializer(DeserializationSchema<T> deserializationSchema) {
            this.deserializationSchema = deserializationSchema;
        }

        @Override
        public ByteBuffer serialize(T value) {
            throw new IllegalStateException("serialize() called within a deserializer");
        }

        @Override
        public T deserialize(ByteBuffer serializedValue) {
            try {
                return deserializationSchema.deserialize(serializedValue.array());
            } catch (IOException e) {
                // Converting exception since the base method doesn't handle checked exceptions.
                throw new RuntimeException(e);
            }
        }
    }
}
