/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream.records;

import com.google.common.collect.ImmutableMap;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * This is data class for storing stream cut with time when the cut was computed.
 * And the size of data being cut.
 * This record is indexed in retentionSet using the recording time and recording size.
 */
@Data
@Slf4j
public class StreamCutRecord {
    public static final RetentionStreamCutRecordSerializer SERIALIZER = new RetentionStreamCutRecordSerializer();

    /**
     * Time when this stream cut was recorded.
     */
    final long recordingTime;
    /**
     * Amount of data in the stream preceeding this cut.
     */
    final long recordingSize;
    /**
     * Actual Stream cut.
     */
    final Map<Long, Long> streamCut;

    @Builder
    /**
     * This is a private constructor that is only directly used by the builder during the deserialization. 
     * The deserialization passes @param copyCollections as false so that we do not make an immutable copy of the collection
     * for the collection passed to the constructor via deserialization. 
     *
     * The all other constructors, the value of copyCollections flag is true and we make an immutable collection copy of 
     * the supplied collection. 
     * All getters of this class that return a collection always wrap them under Collections.unmodifiableCollection so that
     * no one can change the data object from outside.  
     */
    private StreamCutRecord(long recordingTime, long recordingSize, Map<Long, Long> streamCut, boolean copyCollections) {
        this.recordingTime = recordingTime;
        this.recordingSize = recordingSize;
        this.streamCut = copyCollections ? ImmutableMap.copyOf(streamCut) : streamCut;
    }

    public StreamCutRecord(long recordingTime, long recordingSize, Map<Long, Long> streamCut) {
        this(recordingTime, recordingSize, streamCut, true);
    }

    public StreamCutReferenceRecord getReferenceRecord() {
        return new StreamCutReferenceRecord(recordingTime, recordingSize);
    }

    public Map<Long, Long> getStreamCut() {
        return Collections.unmodifiableMap(streamCut);
    }

    private static class StreamCutRecordBuilder implements ObjectBuilder<StreamCutRecord> {

    }

    @SneakyThrows(IOException.class)
    public static StreamCutRecord fromBytes(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toBytes() {
        return SERIALIZER.serialize(this).getCopy();
    }

    private static class RetentionStreamCutRecordSerializer
            extends VersionedSerializer.WithBuilder<StreamCutRecord, StreamCutRecordBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput, StreamCutRecordBuilder streamCutRecordBuilder)
                throws IOException {
            streamCutRecordBuilder.recordingTime(revisionDataInput.readLong())
                                  .recordingSize(revisionDataInput.readLong())
                                  .streamCut(revisionDataInput.readMap(DataInput::readLong, DataInput::readLong))
                                  .copyCollections(false);
        }

        private void write00(StreamCutRecord streamCutRecord, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeLong(streamCutRecord.getRecordingTime());
            revisionDataOutput.writeLong(streamCutRecord.getRecordingSize());
            revisionDataOutput.writeMap(streamCutRecord.getStreamCut(), DataOutput::writeLong, DataOutput::writeLong);
        }

        @Override
        protected StreamCutRecordBuilder newBuilder() {
            return StreamCutRecord.builder();
        }
    }

}
