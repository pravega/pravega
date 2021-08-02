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
package io.pravega.client.tables.impl;

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.ByteBufferUtils;
import io.pravega.shared.protocol.netty.WireCommands;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.SneakyThrows;

/**
 * Key Version for {@link TableSegmentKey}s.
 */
@Getter
@EqualsAndHashCode
public class TableSegmentKeyVersion implements Serializable {
    /**
     * A special KeyVersion which indicates the Key must not exist when performing Conditional Updates.
     */
    public static final TableSegmentKeyVersion NOT_EXISTS = new NotExists();
    /**
     * A special KeyVersion which indicates that no versioning is required.
     */
    public static final TableSegmentKeyVersion NO_VERSION = new TableSegmentKeyVersion(WireCommands.TableKey.NO_VERSION);

    private static final Serializer SERIALIZER = new Serializer();
    private final long segmentVersion;

    @Builder
    private TableSegmentKeyVersion(long segmentVersion) {
        this.segmentVersion = segmentVersion;
    }

    /**
     * Creates a new {@link TableSegmentKeyVersion} from the given value, or returns {@link #NO_VERSION} or
     * {@link #NOT_EXISTS} if the given value is a special version.
     *
     * @param segmentVersion The segment version to wrap.
     * @return A {@link TableSegmentKeyVersion} instance.
     */
    public static TableSegmentKeyVersion from(long segmentVersion) {
        if (segmentVersion == NOT_EXISTS.getSegmentVersion()) {
            return NOT_EXISTS;
        } else if (segmentVersion == NO_VERSION.getSegmentVersion()) {
            return NO_VERSION;
        }

        return new TableSegmentKeyVersion(segmentVersion);
    }

    //region Serialization

    @SneakyThrows(IOException.class)
    public ByteBuffer toBytes() {
        ByteArraySegment serialized = SERIALIZER.serialize(this);
        return ByteBuffer.wrap(serialized.array(), serialized.arrayOffset(), serialized.getLength());
    }

    /**
     * Deserializes the KeyVersion from its serialized form obtained from calling {@link #toBytes()}.
     *
     * @param serializedKeyVersion A serialized TableSegmentKeyVersion.
     * @return The KeyVersion object.
     */
    @SneakyThrows(IOException.class)
    public static TableSegmentKeyVersion fromBytes(ByteBuffer serializedKeyVersion) {
        if (!serializedKeyVersion.hasRemaining()) {
            return NOT_EXISTS;
        }

        return SERIALIZER.deserialize(new ByteArraySegment(serializedKeyVersion));
    }

    private static class TableSegmentKeyVersionBuilder implements ObjectBuilder<TableSegmentKeyVersion> {
    }

    static class Serializer extends VersionedSerializer.WithBuilder<TableSegmentKeyVersion, TableSegmentKeyVersionBuilder> {
        @Override
        protected TableSegmentKeyVersionBuilder newBuilder() {
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

        private void read00(RevisionDataInput revisionDataInput, TableSegmentKeyVersionBuilder builder) throws IOException {
            builder.segmentVersion(revisionDataInput.readLong());
        }

        private void write00(TableSegmentKeyVersion version, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeLong(version.getSegmentVersion());
        }
    }

    /**
     * The object returned by this method is serialized to the object stream. This method is invoked when
     * {@link java.io.ObjectOutputStream} is preparing to write the object to the stream.
     */
    @SneakyThrows(IOException.class)
    private Object writeReplace() {
        return new TableSegmentKeyVersion.SerializedForm(SERIALIZER.serialize(this).getCopy());
    }

    @Data
    private static class SerializedForm implements Serializable {
        private static final long serialVersionUID = 1L;
        private final byte[] value;

        @SneakyThrows(IOException.class)
        Object readResolve() {
            return SERIALIZER.deserialize(new ByteArraySegment(value));
        }
    }

    //endregion

    //region NotExists

    private static class NotExists extends TableSegmentKeyVersion {
        NotExists() {
            super(WireCommands.TableKey.NOT_EXISTS);
        }

        @Override
        public ByteBuffer toBytes() {
            return ByteBufferUtils.EMPTY;
        }

        @Override
        public String toString() {
            return "NOT_EXISTS";
        }

        private Object readResolve() {
            return NOT_EXISTS;
        }
    }

    //endregion
}

