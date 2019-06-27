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
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.GlobalEventExecutor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Encapsulates a {@link ChannelGroup} object for maintaining a group of {@link Channel} objects and common operations
 * performed on the group.
 */
@Slf4j
public class Channels {

    private Lock sequential = new ReentrantLock();

    // See info about ChannelGroups here: https://netty.io/4.1/api/io/netty/channel/group/ChannelGroup.html
    private ChannelGroup channels =
            new DefaultChannelGroup(GlobalEventExecutor.INSTANCE, true);

    public void add(Channel ch) {
        sequential.lock();
        try {
            channels.add(ch);
            log.debug("Done adding channel [{}] to the group.", ch);
        } finally {
            sequential.unlock();
        }
    }

    public void flushStopAndRefresh() {
        log.debug("Flushing, stopping and refreshing chanel group.");
        sequential.lock();
        ChannelGroup existingChannelGroup;
        try {
            existingChannelGroup = channels;
            log.debug("Current instance of Channel Group: {}", channels);
            channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE, true);
            log.debug("New instance of Channel Group: {}", channels);
        } finally {
            sequential.unlock();
        }

        // Flush and close all channels
        existingChannelGroup.flush();
        log.debug("Done triggering flush of channel group {}", existingChannelGroup);
        ChannelGroupFuture future = existingChannelGroup.close();
        try {
            future.await();
        } catch (InterruptedException e) {
            log.warn(e.getMessage(), e);
            // Ignore
        }
        log.debug("Done triggering close of channel group {}", existingChannelGroup);

        if (future.isCancelled()) {
            log.info("Connection cancelled by user.");
        } else if (!future.isSuccess()) {
            Throwable e = future.cause();
            log.warn(e.getMessage(), e);
            // Ignore. Intentionally not rethrowing the exception, so that the system continues to function.
        } else {
            log.info("Done closing the channel group, which implies that all channels in the group were "
                    + "successfully closed.");
        }
    }
}
