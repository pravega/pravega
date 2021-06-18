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

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pravega.client.tables.BadKeyVersionException;
import io.pravega.client.tables.KeyValueTable;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.client.tables.Put;
import io.pravega.client.tables.Remove;
import io.pravega.client.tables.TableEntry;
import io.pravega.client.tables.TableEntryUpdate;
import io.pravega.client.tables.TableKey;
import io.pravega.client.tables.Version;
import java.nio.ByteBuffer;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;

/**
 * Internal helper for translating between Table Segment Keys and Entries and Key-Value Table Keys and Entries.
 */
@Getter
class TableEntryHelper {
    private final SegmentSelector selector;
    private final KeyValueTableConfiguration config;
    private final int totalKeyLength;

    TableEntryHelper(@NonNull SegmentSelector selector, @NonNull KeyValueTableConfiguration config) {
        this.selector = selector;
        this.config = config;
        this.totalKeyLength = this.config.getTotalKeyLength();
    }

    TableSegmentKey toTableSegmentKey(TableSegment tableSegment, Remove removal) {
        validateKeyVersionSegment(tableSegment, removal.getVersion());
        return toTableSegmentKey(serializeKey(removal.getKey()), removal.getVersion());
    }

    private TableSegmentKey toTableSegmentKey(ByteBuf key, Version keyVersion) {
        return new TableSegmentKey(key, toTableSegmentVersion(keyVersion));
    }

    TableKey fromTableSegmentKey(TableSegmentKey key) {
        DeserializedKey rawKey = deserializeKey(key.getKey());
        return new TableKey(rawKey.primaryKey, rawKey.secondaryKey);
    }

    TableSegmentEntry toTableSegmentEntry(TableSegment tableSegment, TableEntryUpdate update) {
        TableKey key = update.getKey();
        if (update instanceof Put) {
            validateKeyVersionSegment(tableSegment, update.getVersion());
        }
        return new TableSegmentEntry(toTableSegmentKey(serializeKey(key), update.getVersion()), serializeValue(update.getValue()));
    }

    TableEntry fromTableSegmentEntry(TableSegment s, TableSegmentEntry e) {
        if (e == null) {
            return null;
        }

        Version version = new VersionImpl(s.getSegmentId(), e.getKey().getVersion());
        TableKey key = fromTableSegmentKey(e.getKey());
        ByteBuffer value = deserializeValue(e.getValue());
        return new TableEntry(key, version, value);
    }

    private TableSegmentKeyVersion toTableSegmentVersion(Version version) {
        return version == null ? TableSegmentKeyVersion.NO_VERSION : TableSegmentKeyVersion.from(version.asImpl().getSegmentVersion());
    }


    ByteBuf serializeKey(TableKey k) {
        return serializeKey(k.getPrimaryKey(), k.getSecondaryKey());
    }

    ByteBuf serializeKey(ByteBuffer primaryKey, ByteBuffer secondaryKey) {
        Preconditions.checkArgument(primaryKey.remaining() == this.config.getPrimaryKeyLength(),
                "Invalid Primary Key Length. Expected %s, actual %s.", this.config.getPrimaryKeyLength(), primaryKey.remaining());
        if (this.config.getSecondaryKeyLength() == 0) {
            Preconditions.checkArgument(secondaryKey == null || secondaryKey.remaining() == this.config.getSecondaryKeyLength(),
                    "Not expecting a Secondary Key.");
            return Unpooled.wrappedBuffer(primaryKey);
        } else {
            Preconditions.checkArgument(secondaryKey.remaining() == this.config.getSecondaryKeyLength(),
                    "Invalid Secondary Key Length. Expected %s, actual %s.", this.config.getSecondaryKeyLength(), secondaryKey.remaining());
            return Unpooled.wrappedBuffer(primaryKey, secondaryKey);
        }
    }

    private DeserializedKey deserializeKey(ByteBuf keySerialization) {
        Preconditions.checkArgument(keySerialization.readableBytes() == this.totalKeyLength,
                "Unexpected key length read back. Expected %s, found %s.", this.totalKeyLength, keySerialization.readableBytes());
        val pk = keySerialization.slice(0, this.config.getPrimaryKeyLength()).copy().nioBuffer();
        val sk = keySerialization.slice(this.config.getPrimaryKeyLength(), this.config.getSecondaryKeyLength()).copy().nioBuffer();
        keySerialization.release(); // Safe to do so now - we made copies of the original buffer.
        return new DeserializedKey(pk, sk);
    }

    private ByteBuf serializeValue(ByteBuffer v) {
        Preconditions.checkArgument(v.remaining() <= KeyValueTable.MAXIMUM_VALUE_LENGTH,
                "Value Too Long. Expected at most %s, actual %s.", KeyValueTable.MAXIMUM_VALUE_LENGTH, v.remaining());
        return Unpooled.wrappedBuffer(v);
    }

    private ByteBuffer deserializeValue(ByteBuf s) {
        val result = s.copy().nioBuffer();
        s.release();
        return result;
    }

    @SneakyThrows(BadKeyVersionException.class)
    private void validateKeyVersionSegment(TableSegment ts, Version version) {
        if (version == null) {
            return;
        }

        VersionImpl impl = version.asImpl();
        boolean valid = impl.getSegmentId() == VersionImpl.NO_SEGMENT_ID || ts.getSegmentId() == impl.getSegmentId();
        if (!valid) {
            throw new BadKeyVersionException(this.selector.getKvt().getScopedName(), "Wrong TableSegment.");
        }
    }

    @RequiredArgsConstructor
    private static class DeserializedKey {
        final ByteBuffer primaryKey;
        final ByteBuffer secondaryKey;
    }
}
