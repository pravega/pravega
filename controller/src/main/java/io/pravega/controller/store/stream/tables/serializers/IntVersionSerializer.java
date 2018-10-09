/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream.tables.serializers;

import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;

import java.io.IOException;

import static io.pravega.controller.store.stream.Version.IntVersion;

public class IntVersionSerializer
        extends VersionedSerializer.WithBuilder<IntVersion, IntVersion.IntVersionBuilder> {
    @Override
    protected byte getWriteVersion() {
        return 0;
    }

    @Override
    protected void declareVersions() {
        version(0).revision(0, this::write00, this::read00);
    }

    private void read00(RevisionDataInput revisionDataInput, IntVersion.IntVersionBuilder builder)
            throws IOException {
        builder.intValue(revisionDataInput.readInt());
    }

    private void write00(IntVersion record, RevisionDataOutput revisionDataOutput) throws IOException {
        revisionDataOutput.writeInt(record.getIntValue());
    }

    @Override
    protected IntVersion.IntVersionBuilder newBuilder() {
        return IntVersion.builder();
    }
}
