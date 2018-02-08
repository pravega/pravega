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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import lombok.Cleanup;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;

public abstract class VersionedSerializer<TargetType, ReaderType> {
    //region Members

    // TODO: add serialization format version.
    // Format (All)     : V(1)|RevisionCount(1)|Revision1|...|Revision[RevisionCount]
    // Format (Revision): RevisionId(1)|Length(4)|Data(Length)

    private final HashMap<Byte, FormatVersion<TargetType, ReaderType>> versions;

    //endregion

    //region Constructor

    private VersionedSerializer() {
        this.versions = new HashMap<>();
        getVersions().forEach(this::registerVersion);
        Preconditions.checkArgument(getFormat(writeVersion()) != null, "Write version %s is not defined.", writeVersion());
    }

    //endregion

    //region Version Registration

    protected abstract byte writeVersion();

    protected abstract Collection<FormatVersion<TargetType, ReaderType>> getVersions();

    FormatVersion<TargetType, ReaderType> getFormat(byte version) {
        return this.versions.get(version);
    }

    private void registerVersion(FormatVersion<TargetType, ReaderType> v) {
        Preconditions.checkArgument(this.versions.put(v.getVersion(), v) == null, "Version %s is already defined.", v.getVersion());
    }

    protected FormatVersion<TargetType, ReaderType> version(int version) {
        return new FormatVersion<>(version);
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
        val writeVersion = getFormat(writeVersion());

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
        val readVersion = getFormat(version);
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

    //region Versions and Revisions

    @Getter
    protected static class FormatVersion<TargetType, ReaderType> {
        private final byte version;
        private final ArrayList<FormatRevision<TargetType, ReaderType>> revisions = new ArrayList<>();

        private FormatVersion(int version) {
            Preconditions.checkArgument(version >= 0 && version <= Byte.MAX_VALUE, "Version must be a value between 0 and ", Byte.MAX_VALUE);
            this.version = (byte) version;
        }

        public FormatVersion<TargetType, ReaderType> revision(int revision, StreamWriter<TargetType> writer, StreamReader<ReaderType> reader) {
            Preconditions.checkNotNull(writer, "writer");
            Preconditions.checkNotNull(reader, "reader");
            Preconditions.checkArgument(revision >= 0 && revision <= Byte.MAX_VALUE,
                    "Revision must be a non-negative value and less than or equal to %s.", Byte.MAX_VALUE);
            Preconditions.checkArgument(this.revisions.isEmpty() || revision == this.revisions.get(this.revisions.size() - 1).getRevision() + 1,
                    "Expected revision to be incremental.");
            this.revisions.add(new FormatRevision<>((byte) revision, writer, reader));
            return this;
        }

        FormatRevision<TargetType, ReaderType> getAtIndex(int index) {
            return index < this.revisions.size() ? this.revisions.get(index) : null;
        }
    }

    @RequiredArgsConstructor
    @Getter
    private static class FormatRevision<TargetType, ReaderType> {
        private final byte revision;
        private final StreamWriter<TargetType> writer;
        private final StreamReader<ReaderType> reader;
    }

    @FunctionalInterface
    protected interface StreamWriter<TargetType> {
        void accept(TargetType source, RevisionDataOutput output) throws IOException;
    }

    @FunctionalInterface
    protected interface StreamReader<TargetType> {
        void accept(RevisionDataInput input, TargetType target) throws IOException;
    }

    //endregion

    //region Types

    public static abstract class Direct<TargetType> extends VersionedSerializer<TargetType, TargetType> {
    }

    public static abstract class WithBuilder<TargetType, ReaderType extends ObjectBuilder<TargetType>> extends VersionedSerializer<TargetType, ReaderType> {
        protected abstract ReaderType newBuilder();

        public TargetType deserialize(RevisionDataInput dataInput) throws IOException {
            return deserialize(dataInput.asStream());
        }

        public TargetType deserialize(InputStream stream) throws IOException {
            ReaderType builder = newBuilder();
            deserialize(stream, builder);
            return builder.build();
        }
    }

    //endregion
}
