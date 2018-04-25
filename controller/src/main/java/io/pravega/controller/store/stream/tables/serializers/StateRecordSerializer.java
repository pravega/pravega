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
import io.pravega.controller.store.stream.tables.State;
import io.pravega.controller.store.stream.tables.StateRecord;

import java.io.IOException;

public class StateRecordSerializer extends VersionedSerializer.WithBuilder<StateRecord, StateRecord.StateRecordBuilder> {
    @Override
    protected byte getWriteVersion() {
        return 0;
    }

    @Override
    protected void declareVersions() {
        version(0).revision(0, this::write00, this::read00);
    }

    private void read00(RevisionDataInput revisionDataInput, StateRecord.StateRecordBuilder builder) throws IOException {
        int ordinal = revisionDataInput.readCompactInt();
        if (ordinal < State.values().length) {
            builder.state(State.values()[ordinal]);
        } else {
            // TODO: what happens if newer states are added and we are trying to deserialize a higher revisioned data from
            // lower revision code for an enum.
            // We are going to throw a meaningful exception here. Choosing a default like UNKNOWN may not be a good idea
            // because state represents some ongoing processing. And we cannot abandon any processing mid way lest we
            // introduce inconsistencies in our metadata.
            throw new VersionMismatchException(StateRecord.class.getName());
        }
    }

    private void write00(StateRecord state, RevisionDataOutput revisionDataOutput) throws IOException {
        revisionDataOutput.writeCompactInt(state.getState().ordinal());
    }

    @Override
    protected StateRecord.StateRecordBuilder newBuilder() {
        return StateRecord.builder();
    }
}
