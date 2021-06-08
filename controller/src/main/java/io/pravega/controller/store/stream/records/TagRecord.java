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
package io.pravega.controller.store.stream.records;

import com.google.common.base.Preconditions;
import io.pravega.client.tables.impl.TableSegment;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.ByteBufferOutputStream;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.ByteArraySegment;
import lombok.Builder;
import lombok.Cleanup;
import lombok.Data;
import lombok.Singular;
import lombok.SneakyThrows;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

/**
 * Data class for Tag Record.
 */
@Data
@Builder(toBuilder = true)
public class TagRecord {

    private static final TagRecordSerializer SERIALIZER = new TagRecordSerializer();
    private final String tagName;
    @Singular
    private final Set<String> streams;

    public static class TagRecordBuilder implements ObjectBuilder<TagRecord> {
        public TagRecordBuilder removeStream(String stream) {
            this.streams.remove(stream);
            return this;
        }
    }

    @SneakyThrows(IOException.class)
    public static TagRecord fromBytes(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toBytes() {
        byte[] bytes = SERIALIZER.serialize(this).getCopy();
        assert bytes.length < TableSegment.MAXIMUM_VALUE_LENGTH : "TagRecord is greater than the TableSegment Maximum value length";
        return SERIALIZER.serialize(this).getCopy();
    }
    
    private static class TagRecordSerializer
            extends VersionedSerializer.WithBuilder<TagRecord,
            TagRecord.TagRecordBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        @Override
        protected void beforeSerialization(TagRecord streamConfigurationRecord) {
            Preconditions.checkNotNull(streamConfigurationRecord);
            Preconditions.checkNotNull(streamConfigurationRecord.getTagName());
        }

        private void read00(RevisionDataInput revisionDataInput, TagRecord.TagRecordBuilder configurationRecordBuilder)
                throws IOException {
            configurationRecordBuilder.tagName(revisionDataInput.readUTF());
            configurationRecordBuilder.streams(decompressArrayOption(revisionDataInput.readArray()));
        }

        private void write00(TagRecord streamConfigurationRecord, RevisionDataOutput revisionDataOutput)
                throws IOException {
            revisionDataOutput.writeUTF(streamConfigurationRecord.getTagName());
            revisionDataOutput.writeBuffer(compressArrayOption(streamConfigurationRecord.getStreams()));
        }

        @Override
        protected TagRecord.TagRecordBuilder newBuilder() {
            return TagRecord.builder();
        }
    }

    private static ByteArraySegment compressArrayOption(final Set<String> tags) throws IOException {
        ByteBufferOutputStream baos = new ByteBufferOutputStream();
        DataOutputStream dout = new DataOutputStream(new DeflaterOutputStream(baos));
        for (String t : tags) {
            dout.writeUTF(t);
        }
        dout.flush();
        dout.close();
        return baos.getData();
    }

    private static Collection<String> decompressArrayOption(final byte[] compressed) throws IOException {
        InputStream in = new InflaterInputStream(new ByteArrayInputStream(compressed));
        @Cleanup
        DataInputStream din = new DataInputStream(in);
        List<String> tags = new ArrayList<>();
        while (true) {
            try {
                tags.add(din.readUTF());
            } catch (EOFException e) {
                break;
            }
        }
        return tags;
    }
}
