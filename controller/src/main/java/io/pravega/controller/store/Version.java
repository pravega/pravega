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
package io.pravega.controller.store;

import io.pravega.client.tables.impl.TableSegmentKeyVersion;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;
import java.io.IOException;

/**
 * Interface to capture version of a metadata object.
 * This version is exposed to processors over the stream metadata store interface
 * and they should use it if they want to perform compare and swap ability over metadata record updates.
 */
public interface Version extends Comparable<Version> {
    IntVersion asIntVersion();

    LongVersion asLongVersion();

    byte[] toBytes();

    abstract class UnsupportedVersion implements Version {
        @Override
        public IntVersion asIntVersion() {
            throw new UnsupportedOperationException();
        }

        @Override
        public LongVersion asLongVersion() {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] toBytes() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int compareTo(Version o) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * A version implementation that uses integer values.
     */
    @Data
    @Builder
    @EqualsAndHashCode(callSuper = false)
    class IntVersion extends UnsupportedVersion {
        public static final IntVersion EMPTY = IntVersion.builder().intValue(Integer.MIN_VALUE).build();
        static final IntVersionSerializer SERIALIZER = new IntVersionSerializer();
        private final int intValue;

        public static class IntVersionBuilder implements ObjectBuilder<IntVersion> {

        }

        public IntVersion(int version) {
            this.intValue = version;
        }

        @Override
        public IntVersion asIntVersion() {
            return this;
        }

        @Override
        public int compareTo(Version o) {
            return Integer.compare(this.intValue, o.asIntVersion().intValue);
        }

        @Override
        @SneakyThrows(IOException.class)
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

        @SneakyThrows(IOException.class)
        public static IntVersion fromBytes(final byte[] data) {
            return SERIALIZER.deserialize(data);
        }
    }

    class IntVersionSerializer
            extends VersionedSerializer.WithBuilder<IntVersion, IntVersion.IntVersionBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput, IntVersion.IntVersionBuilder builder)
                throws IOException {
            builder.intValue(revisionDataInput.readInt());
        }

        private void write00(IntVersion record, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeInt(record.getIntValue());
        }

        @Override
        protected IntVersion.IntVersionBuilder newBuilder() {
            return IntVersion.builder();
        }
    }

    /**
     * A version implementation that uses integer values.
     */
    @Data
    @Builder
    @EqualsAndHashCode(callSuper = false)
    class LongVersion extends UnsupportedVersion {
        public static final LongVersion EMPTY = LongVersion.builder().longValue(TableSegmentKeyVersion.NOT_EXISTS.getSegmentVersion()).build();
        static final LongVersionSerializer SERIALIZER = new LongVersionSerializer();
        private final long longValue;

        public static class LongVersionBuilder implements ObjectBuilder<LongVersion> {

        }

        public LongVersion(long version) {
            this.longValue = version;
        }

        @Override
        public LongVersion asLongVersion() {
            return this;
        }

        @Override
        public int compareTo(Version o) {
            return Long.compare(this.longValue, o.asLongVersion().longValue);
        }

        @Override
        @SneakyThrows(IOException.class)
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

        @SneakyThrows(IOException.class)
        public static LongVersion fromBytes(final byte[] data) {
            return SERIALIZER.deserialize(data);
        }
    }

    class LongVersionSerializer
            extends VersionedSerializer.WithBuilder<LongVersion, LongVersion.LongVersionBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput, LongVersion.LongVersionBuilder builder)
                throws IOException {
            builder.longValue(revisionDataInput.readLong());
        }

        private void write00(LongVersion record, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeLong(record.getLongValue());
        }

        @Override
        protected LongVersion.LongVersionBuilder newBuilder() {
            return LongVersion.builder();
        }
    }
}
