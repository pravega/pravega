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
package io.pravega.shared.protocol.netty;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import java.io.IOException;

/**
 * Extends {@link ByteBufInputStream} with additional functionality.
 */
public class EnhancedByteBufInputStream extends ByteBufInputStream {
    private final ByteBuf buffer;

    public EnhancedByteBufInputStream(ByteBuf buffer) {
        super(buffer);
        this.buffer = buffer;
    }

    /**
     * Returns a {@link ByteBuf} that is a slice of the underlying {@link ByteBuf} beginning at the current reader index
     * and having the given length. Advances the underlying {@link ByteBuf} reader index by length.
     *
     * The returned {@link ByteBuf} is a slice and not a copy of the data (as opposed from {@link ByteBufInputStream#readFully}).
     * Any changes in the underlying buffer in this range will be reflected in the returned {@link ByteBuf} and viceversa.
     * The returned {@link ByteBuf} will have the same {@link ByteBuf#refCnt()} as the underlying buffer so it is important
     * to {@link ByteBuf#retain()} it if it must be referenced for more than the underlying buffer is.
     *
     * @param length The number of bytes to slice out and advance the read position by.
     * @return The sliced {@link ByteBuf}.
     * @throws IOException If an {@link IOException} occurred.
     */
    public ByteBuf readFully(int length) throws IOException {
        Preconditions.checkArgument(length >= 0 && length <= available(),
                "length must a non-negative number less than %s", available());
        ByteBuf result = this.buffer.slice(this.buffer.readerIndex(), length);
        this.buffer.readerIndex(this.buffer.readerIndex() + length);
        return result;
    }
}
