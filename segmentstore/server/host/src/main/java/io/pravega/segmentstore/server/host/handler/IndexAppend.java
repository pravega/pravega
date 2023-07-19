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
import lombok.Builder;
import lombok.Getter;
import lombok.SneakyThrows;

import java.io.IOException;

/**
 * Index Segment associated with main segment.
 */
@Builder
public class IndexAppend {
    static final IndexAppend.IndexAppendSerializer SERIALIZER = new IndexAppend.IndexAppendSerializer();

    @Getter
    private long eventLength;
    @Getter
    private long eventCount;
    @Getter
    private long timeStamp;

    @SneakyThrows(IOException.class)
    public static IndexAppend fromBytes(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toBytes() {
        return SERIALIZER.serialize(this).getCopy();
    }

    public static class IndexAppendBuilder implements ObjectBuilder<IndexAppend> {
    }

    static class IndexAppendSerializer
            extends VersionedSerializer.WithBuilder<IndexAppend, IndexAppend.IndexAppendBuilder> {

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput, IndexAppend.IndexAppendBuilder indexAppendBuilder) throws IOException {
            indexAppendBuilder.eventLength(revisionDataInput.readLong());
            indexAppendBuilder.eventCount(revisionDataInput.readLong());
            indexAppendBuilder.timeStamp(revisionDataInput.readLong());
        }

        private void write00(IndexAppend indexAppend, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeLong(indexAppend.getEventLength());
            revisionDataOutput.writeLong(indexAppend.getEventCount());
            revisionDataOutput.writeLong(indexAppend.getTimeStamp());
        }

        @Override
        protected IndexAppend.IndexAppendBuilder newBuilder() {
            return IndexAppend.builder();
        }
    }
}