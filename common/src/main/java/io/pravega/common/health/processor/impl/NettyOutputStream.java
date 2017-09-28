/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.health.processor.impl;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Class that wraps a netty connection in an outputstream.
 * This class is used to write the health reports.
 */
public class NettyOutputStream extends OutputStream {
    private final Channel channel;

    public NettyOutputStream(Channel channel) {
        this.channel = channel;
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    @Override
    public void flush() throws IOException {
        channel.flush();
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        ByteBuf f = channel.alloc().buffer(len);
        f.writeBytes(b, off, len);
        channel.write(f);
    }

    @Override
    public  void write(int b) throws IOException {
        ByteBuf f = channel.alloc().buffer(1);
        f.writeByte(b);
        channel.write(f);
    }
}
