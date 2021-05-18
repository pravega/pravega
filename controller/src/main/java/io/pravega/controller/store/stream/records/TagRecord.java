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
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import lombok.Builder;
import lombok.Data;
import lombok.Singular;
import lombok.SneakyThrows;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

@Data
@Builder(toBuilder=true)
public class TagRecord {

    private static final TagRecordSerializer SERIALIZER = new TagRecordSerializer();
    private final String tagName;
    @Singular
    private final Set<String> streams;

    public static class TagRecordBuilder implements ObjectBuilder<TagRecord> {
    }

    @SneakyThrows(IOException.class)
    public static TagRecord fromBytes(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toBytes() {
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

        private void read00(RevisionDataInput revisionDataInput,
                            TagRecord.TagRecordBuilder configurationRecordBuilder)
                throws IOException {
            RevisionDataInput.ElementDeserializer<String> stringDeserializer = RevisionDataInput::readUTF;
            configurationRecordBuilder.tagName(revisionDataInput.readUTF());
            byte[] comp = revisionDataInput.readArray();
            Set<String> set = decompressArray(comp);
            configurationRecordBuilder.streams(set);
//            configurationRecordBuilder.tagName(revisionDataInput.readUTF())
//                                      .streams(revisionDataInput.readCollection(stringDeserializer, TreeSet::new));

        }

        private void write00(TagRecord streamConfigurationRecord, RevisionDataOutput revisionDataOutput)
                throws IOException {
            revisionDataOutput.writeUTF(streamConfigurationRecord.getTagName());
//            revisionDataOutput.writeCollection(streamConfigurationRecord.getStreams(), RevisionDataOutput::writeUTF );
            byte[] comp = compressArray(streamConfigurationRecord.getStreams());
            revisionDataOutput.writeArray(comp);
        }

        @Override
        protected TagRecord.TagRecordBuilder newBuilder() {
            return TagRecord.builder();
        }
    }

    public static byte[] compressArray(final Set<String> set) throws IOException {
        ByteArrayOutputStream obj = new ByteArrayOutputStream();
        GZIPOutputStream gzip = new GZIPOutputStream(obj);
        for (String s: set) {
            gzip.write(s.getBytes("UTF-8"));
            // use comma
            gzip.write('#');
            gzip.flush();
        }
        gzip.close();
        return obj.toByteArray();
    }

    public static Set<String> decompressArray(final byte[] compressed) throws IOException {
        final StringBuilder outStr = new StringBuilder();
        if ((compressed == null) || (compressed.length == 0)) {
            return Collections.emptySet();
        }

        final GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(compressed));
        final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(gis, "UTF-8"));

        String line;
        while ((line = bufferedReader.readLine()) != null) {
            outStr.append(line);
        }
        return Arrays.stream(outStr.toString().split("\\#")).collect(Collectors.toSet());
    }
}
