/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.logs;

import com.google.common.base.Preconditions;
import io.pravega.common.io.FixedByteArrayOutputStream;
import io.pravega.common.io.StreamHelpers;
import io.pravega.common.util.SequencedItemList;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.val;

/**
 * Test LogItem implementation that allows injecting serialization errors.
 */
class TestLogItem implements SequencedItemList.Element {
    @Getter
    private final long sequenceNumber;
    @Getter
    private final byte[] data;
    private double failAfterCompleteRatio;
    private IOException exception;

    TestLogItem(long seqNo, byte[] data) {
        this.sequenceNumber = seqNo;
        this.data = data;
        this.failAfterCompleteRatio = -1;
    }

    TestLogItem(InputStream input) throws IOException {
        DataInputStream dataInput = new DataInputStream(input);
        this.sequenceNumber = dataInput.readLong();
        this.data = new byte[dataInput.readInt()];
        int readBytes = StreamHelpers.readAll(dataInput, this.data, 0, this.data.length);
        assert readBytes == this.data.length
                : "SeqNo " + this.sequenceNumber + ": expected to read " + this.data.length + " bytes, but read " + readBytes;

        this.failAfterCompleteRatio = -1;
    }

    void failSerializationAfterComplete(double ratio, IOException exception) {
        if (exception != null) {
            Preconditions.checkArgument(0 <= ratio && ratio < 1, "ratio");
        }

        this.failAfterCompleteRatio = ratio;
        this.exception = exception;
    }

    @SneakyThrows(IOException.class)
    byte[] getFullSerialization() {
        byte[] result = new byte[Long.BYTES + Integer.BYTES + this.data.length];
        serialize(new FixedByteArrayOutputStream(result, 0, result.length));
        return result;
    }

    private void serialize(OutputStream output) throws IOException {
        DataOutputStream dataOutput = new DataOutputStream(output);
        dataOutput.writeLong(this.sequenceNumber);
        dataOutput.writeInt(this.data.length);
        val ex = this.exception;
        if (ex == null) {
            dataOutput.write(this.data);
        } else {
            int breakPoint = (int) (this.data.length * this.failAfterCompleteRatio);
            dataOutput.write(this.data, 0, breakPoint);
            throw ex;
        }
    }

    @Override
    public int hashCode() {
        return Long.hashCode(this.sequenceNumber);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TestLogItem) {
            TestLogItem other = (TestLogItem) obj;
            boolean match = getSequenceNumber() == other.getSequenceNumber() && this.data.length == other.data.length;
            if (match) {
                for (int i = 0; i < this.data.length; i++) {
                    if (this.data[i] != other.data[i]) {
                        return false;
                    }
                }
            }

            return match;
        }

        return false;
    }

    static class TestLogItemSerializer implements Serializer<TestLogItem> {

        @Override
        public void serialize(OutputStream output, TestLogItem item) throws IOException {
            item.serialize(output);
        }

        @Override
        public TestLogItem deserialize(InputStream input) throws IOException {
            return null;
        }
    }
}