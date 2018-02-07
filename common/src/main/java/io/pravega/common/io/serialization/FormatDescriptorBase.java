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
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

public abstract class FormatDescriptorBase<TargetType> {
    //region Members

    protected final HashMap<Byte, FormatVersion<TargetType, ?>> versions;

    //endregion

    //region Constructor

    FormatDescriptorBase() {
        this.versions = new HashMap<>();
        registerVersions();
        Preconditions.checkArgument(getWriteFormatVersion() != null, "Write version %s is not defined.", writeVersion());
    }

    //endregion

    //region Version Registration

    protected abstract byte writeVersion();

    abstract void registerVersions();

    void registerVersion(FormatVersion<TargetType, ?> v) {
        Preconditions.checkArgument(this.versions.put(v.getVersion(), v) == null, "Version %s is already defined.", v.getVersion());
    }

    FormatVersion<TargetType, ?> getWriteFormatVersion() {
        return this.versions.get(writeVersion());
    }

    //endregion

    //region Nested Classes

    @Getter
    static abstract class FormatVersion<TargetType, ReaderType> {
        private final byte version;
        private final ArrayList<FormatRevision<TargetType, ReaderType>> revisions = new ArrayList<>();

        FormatVersion(int version) {
            Preconditions.checkArgument(version >= 0 && version <= Byte.MAX_VALUE, "Version must be a value between 0 and ", Byte.MAX_VALUE);
            this.version = (byte) version;
        }

        void createRevision(int revision, StreamWriter<TargetType> writer, StreamReader<ReaderType> reader,
                                         RevisionCreator<StreamWriter<TargetType>, StreamReader<ReaderType>, FormatRevision<TargetType, ReaderType>> createRevision) {
            Preconditions.checkNotNull(writer, "writer");
            Preconditions.checkNotNull(reader, "reader");
            Preconditions.checkArgument(revision >= 0 && revision <= Byte.MAX_VALUE,
                    "Revision must be a non-negative value and less than or equal to %s.", Byte.MAX_VALUE);
            Preconditions.checkArgument(this.revisions.isEmpty() || revision == this.revisions.get(this.revisions.size() - 1).getRevision() + 1,
                    "Expected revision to be incremental.");
            this.revisions.add(createRevision.apply((byte) revision, writer, reader));
        }

        FormatRevision<TargetType, ReaderType> getAtIndex(int index) {
            return index < this.revisions.size() ? this.revisions.get(index) : null;
        }
    }

    //endregion

    @FunctionalInterface
    protected interface RevisionCreator<WriterType, ReaderType, RevisionType> {
        RevisionType apply(byte revision, WriterType writer, ReaderType reader);
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
}
