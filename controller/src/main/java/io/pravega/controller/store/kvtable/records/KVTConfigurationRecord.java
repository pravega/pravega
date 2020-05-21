/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.kvtable.records;

import com.google.common.base.Preconditions;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Data
@Builder
@Slf4j
@AllArgsConstructor
public class KVTConfigurationRecord {

    public static final ConfigurationRecordSerializer SERIALIZER = new ConfigurationRecordSerializer();

    @NonNull
    private final String scope;
    @NonNull
    private final String kvtName;
    private final KeyValueTableConfiguration kvtConfiguration;

    public static class KVTableConfigurationRecordBuilder implements ObjectBuilder<KVTConfigurationRecord> {
    }

    @SneakyThrows(IOException.class)
    public static KVTConfigurationRecord fromBytes(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toBytes() {
        return SERIALIZER.serialize(this).getCopy();
    }

    private static class ConfigurationRecordSerializer
            extends VersionedSerializer.WithBuilder<KVTConfigurationRecord,
        KVTableConfigurationRecordBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        @Override
        protected void beforeSerialization(KVTConfigurationRecord kvtConfigurationRecord) {
            Preconditions.checkNotNull(kvtConfigurationRecord);
            Preconditions.checkNotNull(kvtConfigurationRecord.getKvtConfiguration());
        }

        private void read00(RevisionDataInput revisionDataInput,
                            KVTableConfigurationRecordBuilder configurationRecordBuilder)
                throws IOException {
            configurationRecordBuilder.scope(revisionDataInput.readUTF())
                                      .kvtName(revisionDataInput.readUTF());
            KeyValueTableConfiguration config = new KeyValueTableConfiguration(revisionDataInput.readInt());
            configurationRecordBuilder.kvtConfiguration(config);

        }

        private void write00(KVTConfigurationRecord kvtConfigurationRecord, RevisionDataOutput revisionDataOutput)
                throws IOException {
            revisionDataOutput.writeUTF(kvtConfigurationRecord.getScope());
            revisionDataOutput.writeUTF(kvtConfigurationRecord.getKvtName());
            revisionDataOutput.writeInt(kvtConfigurationRecord.getKvtConfiguration().getPartitionCount());
        }


        @Override
        protected KVTableConfigurationRecordBuilder newBuilder() {
            return KVTConfigurationRecord.builder();
        }
    }
}
