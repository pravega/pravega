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
public final class Channels {

    private final Lock sequential = new ReentrantLock();

    // See info about ChannelGroups here: https://netty.io/4.1/api/io/netty/channel/group/ChannelGroup.html
    private ChannelGroup channels =
            new DefaultChannelGroup(GlobalEventExecutor.INSTANCE, true);

    public void add(Channel ch) {
        sequential.lock();
        try {
            channels.add(ch);
            log.debug("Added channel [{}] to the group.", ch);
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
            log.warn("Exception encountered when awaiting for the channel group to close", e);
            // Ignore
        }
        log.debug("Triggered close of channel group {}", existingChannelGroup);

        if (future.isCancelled()) {
            log.info("Connection cancelled by user.");
        } else if (!future.isSuccess()) {
            Throwable e = future.cause();
            log.warn("Exception encountered when attempting to close the channel group {}", existingChannelGroup, e);
            // Ignore. Intentionally not rethrowing the exception, so that the system continues to function.
        } else {
            log.info("Channel group {} is closed", existingChannelGroup);
        }
    }
}
