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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

public abstract class FormatDescriptor<TargetType, ReaderType> {
    //region Members

    private final HashMap<Byte, FormatVersion<TargetType, ReaderType>> versions;

    //endregion

    //region Constructor

    private FormatDescriptor() {
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

    //region Versions and Revisions

    @Getter
    public static class FormatVersion<TargetType, ReaderType> {
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
    static class FormatRevision<TargetType, ReaderType> {
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

    //region Sub-types

    public static abstract class Direct<TargetType> extends FormatDescriptor<TargetType, TargetType> {

    }

    public static abstract class WithBuilder<TargetType, ReaderType extends ObjectBuilder<TargetType>> extends FormatDescriptor<TargetType, ReaderType> {
        protected abstract ReaderType newBuilder();
    }

    //endregion
}
