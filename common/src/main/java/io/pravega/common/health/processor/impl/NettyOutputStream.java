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
    public void write(int b) throws IOException {
        channel.write(b);
    }

    @Override
    public void close() {
        this.channel.flush();
        this.channel.close();
    }
}
