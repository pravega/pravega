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
package io.pravega.service.server.logs.operations;

import io.pravega.common.util.ByteArraySegment;
import io.pravega.service.server.logs.SerializationException;
import com.google.common.base.Preconditions;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Log Operation that contains a checkpoint of th Metadata at a particular point in time.
 */
public class MetadataCheckpointOperation extends MetadataOperation {
    //region Members

    private static final byte CURRENT_VERSION = 0;
    private ByteArraySegment contents;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the MetadataCheckpointOperation class.
     */
    public MetadataCheckpointOperation() {
        super();
    }

    protected MetadataCheckpointOperation(OperationHeader header, DataInputStream source) throws SerializationException {
        super(header, source);
    }

    //endregion

    //region MetadataCheckpointOperation Implementation

    /**
     * Sets the Contents of this MetadataCheckpointOperation.
     *
     * @param contents The contents to set.
     */
    public void setContents(ByteArraySegment contents) {
        Preconditions.checkNotNull(contents, "contents");
        Preconditions.checkState(this.contents == null, "This operation has already had its contents set.");
        this.contents = contents;
    }

    /**
     * Gets the contents of this MetadataCheckpointOperation.
     */
    public ByteArraySegment getContents() {
        return this.contents;
    }

    //endregion

    //region Operation Implementation

    @Override
    protected OperationType getOperationType() {
        return OperationType.MetadataCheckpoint;
    }

    @Override
    protected void serializeContent(DataOutputStream target) throws IOException {
        ensureSerializationCondition(this.contents != null, "contents has not been assigned for this entry.");
        target.writeByte(CURRENT_VERSION);
        target.writeInt(this.contents.getLength());
        this.contents.writeTo(target);
    }

    @Override
    protected void deserializeContent(DataInputStream source) throws IOException, SerializationException {
        readVersion(source, CURRENT_VERSION);
        int contentsLength = source.readInt();
        this.contents = new ByteArraySegment(new byte[contentsLength]);
        int bytesRead = this.contents.readFrom(source);
        assert bytesRead == contentsLength : "StreamHelpers.readAll did not read all the bytes requested.";
    }

    @Override
    public String toString() {
        return String.format("%s, Length = %d", super.toString(), contents == null ? 0 : contents.getLength());
    }

    //endregion
}
