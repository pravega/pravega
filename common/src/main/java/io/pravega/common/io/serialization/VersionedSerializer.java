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

public class VersionedSerializer<TargetType, ReaderType> {
    // TODO: add serialization format version.
    // Format (All): V(1)|RCount(1)|Rev1|...|Rev[RCount]
    // Format (Rev): RevId(1)|Length(4)|Data(Variable)

    private final FormatDescriptor<TargetType, ReaderType> formatDescriptor;

    public VersionedSerializer(FormatDescriptor<TargetType, ReaderType> formatDescriptor) {
        this.formatDescriptor = Preconditions.checkNotNull(formatDescriptor, "formatDescriptor");
    }

    //region Serialize

    // Takes a POJO and serializes to RevisionDataOutput. Simple!
    public void serialize(TargetType target, RevisionDataOutput dataOutput) throws IOException {
        serialize(target, dataOutput.getBaseStream());
    }

    public void serialize(TargetType target, OutputStream stream) throws IOException {
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

    //endregion

    //region Deserialize

    // Takes an InputStream and applies its contents to the given target.
    public void deserialize(RevisionDataInput dataInput, TargetType target) throws IOException {
        deserialize(dataInput.asStream(), target);
    }

    public void deserialize(InputStream stream, TargetType target) throws IOException {
        Preconditions.checkArgument(FormatDescriptor.Direct.class.isAssignableFrom(this.formatDescriptor.getClass()),
                "This method cannot be used if an ObjectBuilder is specified on the FormatDescriptor.");

        val dataInput = stream instanceof DataInputStream ? (DataInputStream) stream : new DataInputStream(stream);
        byte version = dataInput.readByte();
        val readVersion = ((FormatDescriptor.Direct<TargetType>) this.formatDescriptor).getFormat(version);
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

    public TargetType deserialize(RevisionDataInput dataInput) throws IOException {
        return deserialize(dataInput.asStream());
    }

    public <V extends ObjectBuilder<TargetType>> TargetType deserialize(InputStream stream) throws IOException {
        Preconditions.checkArgument(FormatDescriptor.WithBuilder.class.isAssignableFrom(this.formatDescriptor.getClass()),
                "This method requires an ObjectBuilder to be specified on the FormatDescriptor.");

        val dataInput = stream instanceof DataInputStream ? (DataInputStream) stream : new DataInputStream(stream);
        byte version = dataInput.readByte();
        val formatWithBuilder = (FormatDescriptor.WithBuilder<TargetType, V>) this.formatDescriptor;
        val readVersion = formatWithBuilder.getFormat(version);
        checkDataIntegrity(readVersion != null, "Unsupported version %d.", version);

        byte revisionCount = dataInput.readByte();
        checkDataIntegrity(revisionCount >= 0, "Data corruption: negative revision count.");

        int revisionIndex = 0;
        V builder = formatWithBuilder.newBuilder();
        for (int i = 0; i < revisionCount; i++) {
            byte revision = dataInput.readByte();
            val rd = readVersion.getAtIndex(revisionIndex++);
            @Cleanup
            val revisionInput = new RevisionDataInputStream(stream);
            if (rd != null) {
                // We've encountered an unknown revision; we cannot read anymore.
                checkDataIntegrity(revision == rd.getRevision(), "Unexpected revision. Expected %d, found %d.", rd.getRevision(), revision);
                rd.getReader().accept(revisionInput, builder);
            }

            revisionInput.skipRemaining();
        }

        return builder.build();
    }

    private void checkDataIntegrity(boolean condition, String messageFormat, Object... args) throws IOException {
        if (!condition) {
            throw new SerializationException(String.format(messageFormat, args));
        }
    }

    //endregion
}
