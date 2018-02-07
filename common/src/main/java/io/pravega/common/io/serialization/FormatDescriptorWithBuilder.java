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
    @Override
    void registerVersions() {
        getVersions().forEach(super::registerVersion);
    }

    @SuppressWarnings("unchecked")
    FormatVersion<TargetType, BuilderType> getFormat(byte version) {
        return (FormatVersion<TargetType, BuilderType>) this.versions.get(version);
    }

    protected abstract BuilderType newBuilder();

    protected abstract Collection<FormatVersion<TargetType, BuilderType>> getVersions();

    protected FormatVersion<TargetType, BuilderType> newVersion(int version) {
        return new FormatVersion<>(version);
    }
}
