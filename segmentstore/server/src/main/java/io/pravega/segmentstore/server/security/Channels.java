/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.security;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
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

    /**
     * Adds the specified channel to the group.
     *
     * @param channel {@code channel} to be added
     *
     */
    public void add(Channel channel) {
        Preconditions.checkNotNull(channel);
        sequential.lock();
        try {
            channels.add(channel);
            log.debug("Added channel [{}] to the group.", channel);
        } finally {
            sequential.unlock();
        }
    }

    /**
     * Clears the current channels held by this object.
     *
     */
    public void clear() {
        if (!this.channels.isEmpty()) {
            this.channels = this.refresh(this.channels);
        }
    }

    @VisibleForTesting
    ChannelGroup refresh() {
        return refresh(this.get());
    }

    /**
     * Refreshes the specified {@code channelGroup}.
     *
     * @param channelGroup the channelGroup to refresh
     *
     */
    @VisibleForTesting
    ChannelGroup refresh(ChannelGroup channelGroup) {
        Preconditions.checkNotNull(channelGroup);
        Preconditions.checkArgument(!channelGroup.isEmpty());
        log.debug("Flushing, stopping and refreshing chanel group.");

        sequential.lock();

        ChannelGroup existingChannelGroup = null;
        ChannelGroup newChannelGroup = null;
        try {
            existingChannelGroup = channelGroup;
            log.debug("Current instance of Channel Group: {}", existingChannelGroup);
            newChannelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE, true);
            log.debug("New instance of Channel Group: {}", channels);
        } finally {
            sequential.unlock();
        }
        flushAndStop(existingChannelGroup);

        return newChannelGroup;
    }

    @VisibleForTesting
    ChannelGroup get() {
        return this.channels;
    }

    /**
     * Flushes and stops the specified {@code channelGroup}.
     *
     * @param channelGroup
     */
    private void flushAndStop(ChannelGroup channelGroup) {
        // Flush and close all channels
        channelGroup.flush();
        log.debug("Triggered flush of channel group {}", channelGroup);
        ChannelGroupFuture future = channelGroup.close();
        try {
            future.await();
        } catch (InterruptedException e) {
            log.warn("Exception encountered when awaiting for the channel group to close", e);
            // Ignore
        }
        log.debug("Triggered close of channel group {}", channelGroup);

        if (future.isCancelled()) {
            log.info("Connection cancelled by user.");
        } else if (!future.isSuccess()) {
            Throwable e = future.cause();
            log.warn("Exception encountered when attempting to close the channel group {}", channelGroup, e);
            // Ignore. Intentionally not rethrowing the exception, so that the system continues to function.
        } else {
            log.info("Channel group {} is closed", channelGroup);
        }
    }
}
