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
package io.pravega.client.state;

import io.pravega.client.stream.EventWriterConfig;
import java.io.Serializable;

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.ByteArraySegment;
import lombok.Builder;
import lombok.Data;
import java.io.IOException;
import java.nio.ByteBuffer;
import lombok.SneakyThrows;

/**
 * The configuration for a Consistent replicated state synchronizer.
 */
@Data
@Builder
public class SynchronizerConfig implements Serializable {

    private static final long serialVersionUID = 3L;
    private static final SynchronizerConfigSerializer SERIALIZER = new SynchronizerConfigSerializer();

    /**
     * This writer config is used by the segment writers in the StateSynchronizer. The default values
     * enable connection pooling and ensures the background connection retry attempts continue until the StateSynchronizer
     * is closed.
     *
     * @param eventWriterConfig writer config used by the segment writers in the StateSynchronizer
     * @return writer config used by the segment writers in the StateSynchronizer
     */
    EventWriterConfig eventWriterConfig;
    /**
     * This size is used to allocate buffer space for the bytes the reader in the StateSynchronizer reads from the
     * segment. The default buffer size is 256KB.
     *
     * @param readBufferSize Size used to allocate buffer space for the bytes the StateSynchronizer reader reads from the segment.
     * @return Size used to allocate buffer space for the bytes the StateSynchronizer reader reads from the segment.
     */
    int readBufferSize;
    
    public static class SynchronizerConfigBuilder implements ObjectBuilder<SynchronizerConfig> {
        private EventWriterConfig eventWriterConfig = EventWriterConfig.builder().retryAttempts(Integer.MAX_VALUE).enableConnectionPooling(true).build();
        private int readBufferSize = 256 * 1024;
    }

    static class SynchronizerConfigSerializer
            extends VersionedSerializer.WithBuilder<SynchronizerConfig, SynchronizerConfigBuilder> {
        @Override
        protected SynchronizerConfigBuilder newBuilder() {
            return builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput, SynchronizerConfigBuilder builder) throws IOException {
            builder.readBufferSize(revisionDataInput.readInt());
            builder.eventWriterConfig(EventWriterConfig.fromBytes(ByteBuffer.wrap(revisionDataInput.readArray())));
        }

        private void write00(SynchronizerConfig object, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeInt(object.getReadBufferSize());
            ByteBuffer buff = object.eventWriterConfig.toBytes();
            revisionDataOutput.writeArray(object.eventWriterConfig.toBytes().array(), 0, buff.remaining());
        }
    }

    @SneakyThrows(IOException.class)
    public ByteBuffer toBytes() {
        ByteArraySegment serialized = SERIALIZER.serialize(this);
        return ByteBuffer.wrap(serialized.array(), serialized.arrayOffset(), serialized.getLength());
    }

    @SneakyThrows(IOException.class)
    public static SynchronizerConfig fromBytes(ByteBuffer buff) {
        return SERIALIZER.deserialize(new ByteArraySegment(buff));
    }

    @SneakyThrows(IOException.class)
    private Object writeReplace() {
        return new SynchronizerConfig.SerializedForm(SERIALIZER.serialize(this).getCopy());
    }

    @Data
    private static class SerializedForm implements Serializable {
        private static final long serialVersionUID = 3L;
        private final byte[] value;
        @SneakyThrows(IOException.class)
        Object readResolve() {
            return SERIALIZER.deserialize(new ByteArraySegment(value));
        }
    }
}
