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
import java.util.Collection;

public abstract class FormatDescriptorWithBuilder<TargetType, BuilderType extends ObjectBuilder<TargetType>> extends FormatDescriptorBase<TargetType> {
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

    public class FormatVersionWithBuilder extends FormatVersion<TargetType, BuilderType> {

        private FormatVersionWithBuilder(int version) {
            super(version);
        }

        public FormatVersionWithBuilder revision(int revision, StreamWriter<TargetType> writer, StreamReader<BuilderType> readerWithBuilder) {
            createRevision(revision, writer, readerWithBuilder, FormatRevision<TargetType, BuilderType>::new);
            return this;
        }
    }

}
