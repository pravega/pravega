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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nullable;
import lombok.NonNull;
import org.apache.commons.lang3.SerializationException;

/**
 * Serializer for Key-Value Table Key Families.
 */
class KeyFamilySerializer {
    /**
     * Maximum length for a Key Family serialization using {@link #ENCODING}.
     * If this value is changed, care must be taken such that the sum of it, {@link #PREFIX_LENGTH} and
     * {@link TableSegment#MAXIMUM_KEY_LENGTH} do not exceed the server-side limits.
     */
    @VisibleForTesting
    static final int MAX_KEY_FAMILY_LENGTH = 2048;
    /**
     * A prefix added to every Key Family serialization. This is encoded in the key itself. See
     * {@link #MAX_KEY_FAMILY_LENGTH} for constraints.
     */
    @VisibleForTesting
    static final int PREFIX_LENGTH = 2; // [0, 65535]. First 6 bits will be empty.
    @VisibleForTesting
    static final Charset ENCODING = StandardCharsets.UTF_8;

    /**
     * Serializes the given Key Family into a new {@link ByteBuf} with the following format:
     * - Bytes [0, 2): Length of UTF8-encoded Key Family (0 if Key Family is null). (Value is L)
     * - Bytes [2, 2+L): UTF8-encoded Key Family.
     *
     * @param keyFamily Key Family to serialize.
     * @return A new {@link ByteBuf}. If Key Family Length is non-zero, this will likely be a composite {@link ByteBuf}.
     * This buffer will have {@link ByteBuf#readerIndex()} set to 0 and {@link ByteBuf#writerIndex()} set to the end.
     * @throws IllegalArgumentException If the UTF8-encoded Key Family exceeds {@link #MAX_KEY_FAMILY_LENGTH}.
     */
    ByteBuf serialize(@Nullable String keyFamily) {
        ByteBuf prefix = Unpooled.wrappedBuffer(new byte[PREFIX_LENGTH]);
        if (keyFamily == null || keyFamily.length() == 0) {
            prefix.setShort(0, 0);
            return prefix; // Prefix is 00, which is the length of the Key Family
        } else {
            ByteBuffer s = ENCODING.encode(keyFamily);
            Preconditions.checkArgument(s.remaining() <= MAX_KEY_FAMILY_LENGTH,
                    "KeyFamily must have a %s encoding length of at most %s bytes; actual %s.", ENCODING.name(), MAX_KEY_FAMILY_LENGTH, s.remaining());
            prefix.setShort(0, s.remaining());
            return Unpooled.wrappedBuffer(prefix, Unpooled.wrappedBuffer(s));
        }
    }

    /**
     * Deserializes a Key Family serialized using {@link ByteBuf}.
     *
     * @param serializedValue A {@link ByteBuf}. Deserialization will begin from {@link ByteBuf#readerIndex()} and not
     *                        exceed {@link ByteBuf#readableBytes()}. When this method is complete, the {@link ByteBuf#readerIndex()}
     *                        will have advanced by the number of bytes read. See {@link #serialize} for format.
     * @return A String representing the Key Family. Returns null if the serialized Key Family length was 0 (which means
     * that serializing an empty string will also deserialize a null value).
     * @throws SerializationException If the given {@link ByteBuf} has insufficient bytes to accommodate a Key Family or
     *                                if the serialization cannot be parsed.
     */
    String deserialize(@NonNull ByteBuf serializedValue) {
        if (serializedValue.readableBytes() < PREFIX_LENGTH) {
            throw new SerializationException(String.format("At least %d bytes required, given %d.", PREFIX_LENGTH, serializedValue.readableBytes()));
        }

        int kfLength = serializedValue.readShort();
        if (kfLength < 0 || kfLength > MAX_KEY_FAMILY_LENGTH) {
            throw new SerializationException(String.format("Invalid Key Family Length. Expected between %d and %d, read %d.",
                    0, MAX_KEY_FAMILY_LENGTH, kfLength));
        } else if (serializedValue.readableBytes() < kfLength) {
            throw new SerializationException(String.format(
                    "Insufficient bytes remaining in buffer. Expected at least %d, actual %d.", kfLength, serializedValue.readableBytes()));
        } else if (kfLength == 0) {
            // No Key Family
            return null;
        } else {
            String result = ENCODING.decode(serializedValue.nioBuffer(serializedValue.readerIndex(), kfLength)).toString();
            serializedValue.readerIndex(serializedValue.readerIndex() + kfLength);
            return result;
        }
    }
}
