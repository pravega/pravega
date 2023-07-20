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
package io.pravega.segmentstore.server.host.handler;

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import java.io.IOException;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;

/**
 * Index Segment associated with main segment.
 */
@Data
@Builder
public class IndexEntry {
    static final IndexEntry.IndexAppendSerializer SERIALIZER = new IndexEntry.IndexAppendSerializer();

    private long eventLength;
    private long eventCount;
    private long timeStamp;

    @SneakyThrows(IOException.class)
    public static IndexEntry fromBytes(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toBytes() {
        return SERIALIZER.serialize(this).getCopy();
    }

    public static class IndexEntryBuilder implements ObjectBuilder<IndexEntry> {
    }

    static class IndexAppendSerializer
            extends VersionedSerializer.WithBuilder<IndexEntry, IndexEntryBuilder> {

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput, IndexEntryBuilder indexEntryBuilder) throws IOException {
            indexEntryBuilder.eventLength(revisionDataInput.readLong());
            indexEntryBuilder.eventCount(revisionDataInput.readLong());
            indexEntryBuilder.timeStamp(revisionDataInput.readLong());
        }

        private void write00(IndexEntry indexEntry, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeLong(indexEntry.getEventLength());
            revisionDataOutput.writeLong(indexEntry.getEventCount());
            revisionDataOutput.writeLong(indexEntry.getTimeStamp());
        }

        @Override
        protected IndexEntryBuilder newBuilder() {
            return IndexEntry.builder();
        }
    }
}