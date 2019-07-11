/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.protocol.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.pravega.common.util.BufferView;
import java.io.InputStream;
import lombok.NonNull;

/**
 * {@link BufferView} wrapper for {@link ByteBuf} instances.
 */
public class ByteBufWrapper implements BufferView {
    private final ByteBuf buf;

    /**
     * Creates a new instance of the {@link ByteBufWrapper} wrapping the given {@link ByteBuf}.
     *
     * This instance will begin at the given buffer's {@link ByteBuf#readerIndex()} and will have a length equal to
     * {@link ByteBuf#readableBytes()}.
     *
     * @param buf The {@link ByteBuf} to wrap. A read-only duplicate will be made of this buffer; any changes made to the
     *            {@link ByteBuf#readerIndex()} or {@link ByteBuf#writerIndex()} to this object will not be reflected
     *            in this {@link ByteBufWrapper} instance.
     */
    public ByteBufWrapper(@NonNull ByteBuf buf) {
        this.buf = buf.asReadOnly();
    }

    //region BufferView implementation

    @Override
    public int getLength() {
        return this.buf.readableBytes();
    }

    @Override
    public InputStream getReader() {
        return new ByteBufInputStream(this.buf.duplicate(), false);
    }

    @Override
    public byte[] getCopy() {
        ByteBuf buf = this.buf.duplicate();
        byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);
        return bytes;
    }

    @Override
    public String toString() {
        return this.buf.toString();
    }

    //endregion
}
