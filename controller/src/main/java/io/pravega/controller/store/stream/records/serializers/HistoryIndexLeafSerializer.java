/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream.records.serializers;

import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.controller.store.stream.records.HistoryTimeIndexLeaf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

public class HistoryIndexLeafSerializer extends
        VersionedSerializer.WithBuilder<HistoryTimeIndexLeaf, HistoryTimeIndexLeaf.HistoryTimeIndexLeafBuilder> {
    @Override
    protected byte getWriteVersion() {
        return 0;
    }

    @Override
    protected void declareVersions() {
        version(0).revision(0, this::write00, this::read00);
    }

    private void read00(RevisionDataInput revisionDataInput, HistoryTimeIndexLeaf.HistoryTimeIndexLeafBuilder builder) throws IOException {
        builder.records(revisionDataInput.readCollection(DataInput::readLong, ArrayList::new));
    }

    private void write00(HistoryTimeIndexLeaf history, RevisionDataOutput revisionDataOutput) throws IOException {
        revisionDataOutput.writeCollection(history.getRecords(), DataOutput::writeLong);
    }

    @Override
    protected HistoryTimeIndexLeaf.HistoryTimeIndexLeafBuilder newBuilder() {
        return HistoryTimeIndexLeaf.builder();
    }
}