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

import io.pravega.common.ObjectBuilder;
import java.io.IOException;
import java.util.Collection;
import lombok.Getter;

public abstract class FormatDescriptorWithBuilder<T, BuilderType extends ObjectBuilder<T>> extends FormatDescriptorBase<T> {
    protected abstract BuilderType newBuilder();

    @Override
    void registerVersions() {
        getVersions().forEach(super::registerVersion);
    }

    @SuppressWarnings("unchecked")
    FormatVersionWithBuilder getFormat(byte version) {
        return (FormatVersionWithBuilder) this.versions.get(version);
    }

    protected abstract Collection<FormatVersionWithBuilder> getVersions();

    protected FormatVersionWithBuilder newVersion(int version) {
        return new FormatVersionWithBuilder(version);
    }

    public class FormatVersionWithBuilder extends FormatVersion<RevisionWithBuilder> {

        private FormatVersionWithBuilder(int version) {
            super(version);
        }

        public FormatVersionWithBuilder revision(int revision, StreamWriter<T> writer, StreamReaderWithBuilder<BuilderType> readerWithBuilder) {
            createRevision(revision, writer, readerWithBuilder, RevisionWithBuilder::new);
            return this;
        }
    }


    // TODO: can we unify these FormatRevisions into one generic class? I think we can.
    @Getter
    class RevisionWithBuilder extends FormatRevision {
        private final StreamReaderWithBuilder<BuilderType> reader;

        RevisionWithBuilder(byte revision, StreamWriter<T> writer, StreamReaderWithBuilder<BuilderType> reader) {
            super(revision, writer);
            this.reader = reader;
        }
    }

    @FunctionalInterface
    protected interface StreamReaderWithBuilder<TargetBuilder> {
        void accept(RevisionDataInput input, TargetBuilder target) throws IOException;
    }
}
