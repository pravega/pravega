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

import java.util.Collection;

public abstract class FormatDescriptorDirect<T> extends FormatDescriptorBase<T> {

    @Override
    void registerVersions() {
        getVersions().forEach(super::registerVersion);
    }

    @SuppressWarnings("unchecked")
    FormatVersionDirect getFormat(byte version) {
        return (FormatVersionDirect) this.versions.get(version);
    }

    protected abstract Collection<FormatVersionDirect> getVersions();

    protected FormatVersionDirect newVersion(int version) {
        return new FormatVersionDirect(version);
    }

    public class FormatVersionDirect extends FormatVersion<FormatRevision<T>> {
        private FormatVersionDirect(int version) {
            super(version);
        }

        public FormatVersionDirect revision(int revision, StreamWriter<T> writer, StreamReader<T> reader) {
            createRevision(revision, writer, reader, FormatRevision<T>::new);
            return this;
        }
    }
}
