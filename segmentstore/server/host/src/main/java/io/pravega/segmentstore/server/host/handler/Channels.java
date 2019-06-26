/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host.handler;

import io.netty.channel.Channel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.GlobalEventExecutor;

/**
 * Encapsulates a {@link ChannelGroup} object for maintaining a group of {@link Channel} objects and common operations
 * performed on the group.
 */
public class Channels {

    private static final ChannelGroup CHANNEL_GROUP =
            new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    public static synchronized ChannelGroup get() {
        return CHANNEL_GROUP;
    }

    public static synchronized void add(Channel ch) {
        CHANNEL_GROUP.add(ch);
    }
}
