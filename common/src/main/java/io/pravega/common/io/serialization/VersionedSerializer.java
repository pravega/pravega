/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.io.serialization;

import com.google.common.base.Preconditions;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.SerializationException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import lombok.Cleanup;
import lombok.val;

public abstract class VersionedSerializer<TargetType, ReaderType> {
    //region Members

    // TODO: add serialization format version.
    // Format (All)     : V(1)|RevisionCount(1)|Revision1|...|Revision[RevisionCount]
    // Format (Revision): RevisionId(1)|Length(4)|Data(Length)

    protected final FormatDescriptor<TargetType, ReaderType> formatDescriptor;

    //endregion

    //region Constructor

    VersionedSerializer(FormatDescriptor<TargetType, ReaderType> formatDescriptor) {
        this.formatDescriptor = Preconditions.checkNotNull(formatDescriptor, "formatDescriptor");
    }

    public static <TargetType> VersionedSerializer.Direct<TargetType> use(FormatDescriptor.Direct<TargetType> formatDescriptor) {
        return new VersionedSerializer.Direct<>(formatDescriptor);
    }

    public static <TargetType, ReaderType extends ObjectBuilder<TargetType>> VersionedSerializer.WithBuilder<TargetType, ReaderType> use(
            FormatDescriptor.WithBuilder<TargetType, ReaderType> formatDescriptor) {
        return new VersionedSerializer.WithBuilder<>(formatDescriptor);
    }

    //endregion

    //region Serialization

    // Takes a POJO and serializes to RevisionDataOutput. Simple!
    public void serialize(RevisionDataOutput dataOutput, TargetType target) throws IOException {
        serialize(dataOutput.getBaseStream(), target);
    }

    public void serialize(OutputStream stream, TargetType target) throws IOException {
        // Wrap the given stream in a DataOutputStream, but make sure we don't close it, since we don't own it.
        val dataOutput = stream instanceof DataOutputStream ? (DataOutputStream) stream : new DataOutputStream(stream);
        val writeVersion = this.formatDescriptor.getFormat(this.formatDescriptor.writeVersion());

        // Write Serialization Header.
        dataOutput.writeByte(writeVersion.getVersion());
        dataOutput.writeByte(writeVersion.getRevisions().size());

        // Write each Revision for this Version, in turn.
        for (val r : writeVersion.getRevisions()) {
            dataOutput.writeByte(r.getRevision());
            @Cleanup
            val revisionOutput = RevisionDataOutput.wrap(stream);
            r.getWriter().accept(target, revisionOutput);
        }
    }

    private void checkDataIntegrity(boolean condition, String messageFormat, Object... args) throws IOException {
        if (!condition) {
            throw new SerializationException(String.format(messageFormat, args));
        }
    }

    // Takes an InputStream and applies its contents to the given target.
    public void deserialize(RevisionDataInput dataInput, ReaderType target) throws IOException {
        deserialize(dataInput.asStream(), target);
    }

    public void deserialize(InputStream stream, ReaderType target) throws IOException {
        val dataInput = stream instanceof DataInputStream ? (DataInputStream) stream : new DataInputStream(stream);
        byte version = dataInput.readByte();
        val readVersion = this.formatDescriptor.getFormat(version);
        checkDataIntegrity(readVersion != null, "Unsupported version %d.", version);

        byte revisionCount = dataInput.readByte();
        checkDataIntegrity(revisionCount >= 0, "Data corruption: negative revision count.");

        int revisionIndex = 0;
        for (int i = 0; i < revisionCount; i++) {
            byte revision = dataInput.readByte();
            val rd = readVersion.getAtIndex(revisionIndex++);
            @Cleanup
            val revisionInput = new RevisionDataInputStream(stream);
            if (rd != null) {
                // We've encountered an unknown revision; we cannot read anymore.
                checkDataIntegrity(revision == rd.getRevision(), "Unexpected revision. Expected %d, found %d.", rd.getRevision(), revision);
                rd.getReader().accept(revisionInput, target);
            }

            revisionInput.skipRemaining();
        }
    }

    //endregion

    //region Implementations

    public static class Direct<TargetType> extends VersionedSerializer<TargetType, TargetType> {
        private Direct(FormatDescriptor.Direct<TargetType> formatDescriptor) {
            super(formatDescriptor);
        }
    }

    public static class WithBuilder<TargetType, ReaderType extends ObjectBuilder<TargetType>> extends VersionedSerializer<TargetType, ReaderType> {
        private WithBuilder(FormatDescriptor.WithBuilder<TargetType, ReaderType> formatDescriptor) {
            super(formatDescriptor);
        }

        public TargetType deserialize(RevisionDataInput dataInput) throws IOException {
            return deserialize(dataInput.asStream());
        }

        public TargetType deserialize(InputStream stream) throws IOException {
            val formatWithBuilder = (FormatDescriptor.WithBuilder<TargetType, ReaderType>) this.formatDescriptor;
            ReaderType builder = formatWithBuilder.newBuilder();
            deserialize(stream, builder);
            return builder.build();
        }
    }

    //endregion
}
