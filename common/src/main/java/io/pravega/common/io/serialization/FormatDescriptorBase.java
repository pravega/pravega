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
import lombok.val;

public abstract class FormatDescriptorBase<T> {
    //region Members

    protected final HashMap<Byte, FormatVersion<? extends FormatRevision>> versions;

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

    void registerVersion(FormatVersion<? extends FormatRevision> v) {
        Preconditions.checkArgument(this.versions.put(v.getVersion(), v) == null, "Version %s is already defined.", v.getVersion());
    }

    FormatVersion<? extends FormatRevision> getWriteFormatVersion() {
        return this.versions.get(writeVersion());
    }

    //endregion

    //region Nested Classes

    @Getter
    abstract class FormatVersion<RevisionType extends FormatRevision> {
        private final byte version;
        private final ArrayList<RevisionType> revisions = new ArrayList<>();

        FormatVersion(int version) {
            Preconditions.checkArgument(version >= 0 && version <= Byte.MAX_VALUE, "Version must be a value between 0 and ", Byte.MAX_VALUE);
            this.version = (byte) version;
        }

        <ReaderType> void createRevision(int revision, StreamWriter<T> writer, ReaderType reader, RevisionCreator<StreamWriter<T>, ReaderType, RevisionType> createRevision) {
            Preconditions.checkNotNull(writer, "writer");
            Preconditions.checkNotNull(reader, "reader");
            Preconditions.checkArgument(revision >= 0 && revision <= Byte.MAX_VALUE,
                    "Revision must be a non-negative value and less than or equal to %s.", Byte.MAX_VALUE);
            val revisions = getRevisions();
            Preconditions.checkArgument(revisions.isEmpty() || revision == revisions.get(revisions.size() - 1).getRevision() + 1,
                    "Expected revision to be incremental.");
            //revisions.add(new RevisionDirect((byte) revision, writer, reader));
            revisions.add(createRevision.apply((byte) revision, writer, reader));
        }

        RevisionType getAtIndex(int index) {
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
    abstract class FormatRevision {
        private final byte revision;
        private final StreamWriter<T> writer;
    }

    @FunctionalInterface
    protected interface StreamWriter<TargetType> {
        void accept(TargetType source, RevisionDataOutput output) throws IOException;
    }

    //endregion
}
