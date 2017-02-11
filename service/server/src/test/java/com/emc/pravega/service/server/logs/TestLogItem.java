/**
 *  Copyright (c) 2016 Dell Inc. or its subsidiaries. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.emc.pravega.service.server.logs;

import com.emc.pravega.common.io.FixedByteArrayOutputStream;
import com.emc.pravega.common.io.StreamHelpers;
import com.emc.pravega.service.server.LogItem;
import com.google.common.base.Preconditions;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Test LogItem implementation that allows injecting serialization errors.
 */
class TestLogItem implements LogItem {
    private final long seqNo;
    private final byte[] data;
    private double failAfterCompleteRatio;
    private IOException exception;

    public TestLogItem(long seqNo, byte[] data) {
        this.seqNo = seqNo;
        this.data = data;
        this.failAfterCompleteRatio = -1;
    }

    public TestLogItem(InputStream input) throws SerializationException {
        DataInputStream dataInput = new DataInputStream(input);
        try {
            this.seqNo = dataInput.readLong();
            this.data = new byte[dataInput.readInt()];
            int readBytes = StreamHelpers.readAll(dataInput, this.data, 0, this.data.length);
            assert readBytes == this.data.length;
        } catch (IOException ex) {
            throw new SerializationException("TestLogItem.deserialize", ex.getMessage(), ex);
        }
        this.failAfterCompleteRatio = -1;
    }

    public void failSerializationAfterComplete(double ratio, IOException exception) {
        if (exception != null) {
            Preconditions.checkArgument(0 <= ratio && ratio < 1, "ratio");
        }

        this.failAfterCompleteRatio = ratio;
        this.exception = exception;
    }

    public byte[] getData() {
        return this.data;
    }

    public byte[] getFullSerialization() {
        byte[] result = new byte[Long.BYTES + Integer.BYTES + this.data.length];
        try {
            this.serialize(new FixedByteArrayOutputStream(result, 0, result.length));
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        return result;
    }

    @Override
    public long getSequenceNumber() {
        return this.seqNo;
    }

    @Override
    public void serialize(OutputStream output) throws IOException {
        DataOutputStream dataOutput = new DataOutputStream(output);
        dataOutput.writeLong(seqNo);
        dataOutput.writeInt(data.length);
        if (this.exception == null) {
            dataOutput.write(data);
        } else {
            int breakPoint = (int) (data.length * this.failAfterCompleteRatio);
            dataOutput.write(data, 0, breakPoint);
            throw this.exception;
        }
    }
}