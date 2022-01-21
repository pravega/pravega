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
import java.nio.charset.Charset;
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
     * This writer config is used by the segment writers in the StateSyncrhonizer. The default values
     * enable connection pooling and ensures the background connection retry attempts continue until the StateSyncrhonizer
     * is closed.
     */
    EventWriterConfig eventWriterConfig;
    /**
     * This size is used to allocate buffer space for the bytes the reader in the StateSyncrhonizer reads from the
     * segment. The default buffer size is 256KB.
     */
    int readBufferSize;
    
    public static class SynchronizerConfigBuilder implements ObjectBuilder<SynchronizerConfig> {
        private EventWriterConfig eventWriterConfig = EventWriterConfig.builder().retryAttempts(Integer.MAX_VALUE).enableConnectionPooling(true).build();
        private int readBufferSize = 256 * 1024;
    }

    private static class SynchronizerConfigSerializer
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
            byte[] b = new byte[0];
            builder.readBufferSize(revisionDataInput.readInt());
            revisionDataInput.readFully(b);
            builder.eventWriterConfig(builder.eventWriterConfig.fromBytes(ByteBuffer.wrap(b)));
        }

        private void write00(SynchronizerConfig object, RevisionDataOutput revisionDataOutput) throws IOException {
            Charset cs = Charset.forName("UTF-8");
            revisionDataOutput.writeInt(object.getReadBufferSize());
            revisionDataOutput.writeBytes(cs.decode(object.eventWriterConfig.toBytes()).toString());
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
}
