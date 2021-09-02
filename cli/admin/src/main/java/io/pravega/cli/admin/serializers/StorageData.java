/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.cli.admin.serializers;

import io.pravega.client.stream.Serializer;
import io.pravega.common.ObjectBuilder;
import io.pravega.segmentstore.storage.metadata.StorageMetadata;
import lombok.Builder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

@Builder
public class StorageData implements Serializable {
    /**
     * Version. This version number is independent of version in the store.
     * This is required to keep track of all modifications to data when it is changed while still in buffer without writing it to database.
     */
    private final long version;

    /**
     * Key of the record.
     */
    private final String key;

    /**
     * Value of the record.
     */
    private final StorageMetadata value;

    public static class StorageDataBuilder implements ObjectBuilder<StorageData> {
    }

    public static class StorageDataSerializer implements Serializer<StorageData> {

        /**
         * Serializer for {@link StorageMetadata}.
         */
        StorageMetadata.StorageMetadataSerializer SERIALIZER = new StorageMetadata.StorageMetadataSerializer();

        @Override
        public ByteBuffer serialize(StorageData value) {
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            ObjectOutputStream out;
            try {
                out = new ObjectOutputStream(bout);
                out.writeLong(value.version);
                out.writeUTF(value.key);
                boolean hasValue = value.value != null;
                if (hasValue) {
                    SERIALIZER.serialize(value.value);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return ByteBuffer.wrap(bout.toByteArray());
        }

        @Override
        public StorageData deserialize(ByteBuffer serializedValue) {
            ByteArrayInputStream bin = new ByteArrayInputStream(serializedValue.array(),
                    serializedValue.position(),
                    serializedValue.remaining());
            ObjectInputStream in;
            try {
                in = new ObjectInputStream(bin);
                StorageData.StorageDataBuilder data = new StorageData.StorageDataBuilder();
                data.version(in.readLong());
                data.key(in.readUTF());
                boolean hasValue = in.readBoolean();
                if (hasValue) {
                    data.value(SERIALIZER.deserialize(in));
                }
                return data.build();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
