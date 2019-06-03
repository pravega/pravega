/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
abstract class CheckpointOperationBase extends MetadataOperation {
    //region Members

    private ByteArraySegment contents;

    //endregion

    //region CheckpointOperationBase Implementation

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
     * Gets the contents of this CheckpointOperationBase.
     * @return the contents of this CheckpointOperationBase.
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