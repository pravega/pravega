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
package io.pravega.segmentstore.server.logs.operations;

import com.google.common.base.Preconditions;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.util.ByteArraySegment;
import java.io.IOException;

/**
 * Base Log Operation for any operation wishing to store a checkpoint.
 */
public abstract class CheckpointOperationBase extends MetadataOperation {
    //region Members

    private ByteArraySegment contents;

    //endregion

    //region CheckpointOperationBase Implementation

    /**
     * Sets the Contents of this Checkpoint Operation.
     *
     * @param contents The contents to set.
     */
    public void setContents(ByteArraySegment contents) {
        Preconditions.checkNotNull(contents, "contents");
        Preconditions.checkState(this.contents == null, "This operation has already had its contents set.");
        this.contents = contents;
    }

    /**
     * Clears the Contents of this Checkpoint Operation. This should only be invoked after this Operation has been serialized
     * and/or processed, otherwise all information stored in it will be lost.
     */
    public void clearContents() {
        this.contents = null;
    }

    /**
     * Gets the contents of this Checkpoint Operation.
     * @return the contents of this Checkpoint Operation.
     */
    public ByteArraySegment getContents() {
        return this.contents;
    }

    @Override
    public String toString() {
        return String.format("%s, Length = %d", super.toString(), contents == null ? 0 : contents.getLength());
    }

    //endregion

    static abstract class SerializerBase<T extends CheckpointOperationBase> extends OperationSerializer<T> {
        @Override
        protected abstract OperationBuilder<T> newBuilder();

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        @Override
        protected void beforeSerialization(T o) {
            super.beforeSerialization(o);
            Preconditions.checkState(o.getContents() != null, "Contents has not been assigned.");
        }

        private void write00(T o, RevisionDataOutput target) throws IOException {
            ByteArraySegment c = o.getContents();
            target.length(Long.BYTES + target.getCompactIntLength(c.getLength()) + c.getLength());
            target.writeLong(o.getSequenceNumber());
            target.writeArray(c.array(), c.arrayOffset(), c.getLength());
        }

        private void read00(RevisionDataInput source, OperationBuilder<T> b) throws IOException {
            b.instance.setSequenceNumber(source.readLong());
            b.instance.setContents(new ByteArraySegment(source.readArray()));
        }
    }
}