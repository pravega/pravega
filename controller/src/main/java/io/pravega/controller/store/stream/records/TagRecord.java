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
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import lombok.Builder;
import lombok.Cleanup;
import lombok.Data;
import lombok.Singular;
import lombok.SneakyThrows;
import org.apache.commons.io.IOUtils;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.InflaterInputStream;

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
            byte[] comp = revisionDataInput.readArray();
            Set<String> set = decompressArrayOption(comp);
            configurationRecordBuilder.streams(set);
        }

        private void write00(TagRecord streamConfigurationRecord, RevisionDataOutput revisionDataOutput)
                throws IOException {
            revisionDataOutput.writeUTF(streamConfigurationRecord.getTagName());
            byte[] comp = compressArrayOption(streamConfigurationRecord.getStreams());
            revisionDataOutput.writeArray(comp);
        }

        @Override
        protected TagRecord.TagRecordBuilder newBuilder() {
            return TagRecord.builder();
        }
    }

    // This was discarded since it is slower.
    private static byte[] compressArrayOptionA(final Set<String> set) throws IOException {
        ByteArrayOutputStream obj = new ByteArrayOutputStream();
        GZIPOutputStream gzip = new GZIPOutputStream(obj);
        for (String s: set) {
            gzip.write(s.getBytes(StandardCharsets.UTF_8));
            // use comma
            gzip.write(',');
        }
        gzip.flush();
        gzip.close();
        return obj.toByteArray();
    }

    // This was discarded since it is slower.
    private static Set<String> decompressArrayOptionA(final byte[] compressed) throws IOException {
        final StringBuilder outStr = new StringBuilder();
        if ((compressed == null) || (compressed.length == 0)) {
            return Collections.emptySet();
        }
        @Cleanup
        final GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(compressed));
        @Cleanup
        final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(gis, StandardCharsets.UTF_8));

        String line;
        while ((line = bufferedReader.readLine()) != null) {
            outStr.append(line);
        }
        return Arrays.stream(outStr.toString().split(",")).collect(Collectors.toSet());
    }

    private static byte[] compressArrayOption(final Set<String> set) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputStream out = new DeflaterOutputStream(baos);
        for (String s : set) {
            out.write(s.getBytes(StandardCharsets.UTF_8));
            out.write(',');
        }
        out.flush();
        out.close();
        return baos.toByteArray();
    }

    private static Set<String> decompressArrayOption(final byte[] compressed) throws IOException {
        @Cleanup
        InputStream in = new InflaterInputStream(new ByteArrayInputStream(compressed));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        IOUtils.copy(in, baos);
        String streams = baos.toString(StandardCharsets.UTF_8);
        if (streams.isEmpty()) {
            return Collections.emptySet();
        }
        return Arrays.stream(streams.split(",")).collect(Collectors.toSet());
    }
}
