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

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.local.LocalChannel;
import org.junit.Test;

import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class ChannelsTests {

    @Test
    public void testAddChannels() {
        Channels channels = new Channels();
        channels.add(new LocalChannel());
        channels.add(new LocalChannel());

        assertEquals(2, channels.get().size());
    }

    @Test
    public void testClearSucceedsForEmptyChannels() {
        new Channels().clear();
        // No Exception expected
    }

    @Test
    public void testRefreshFailsForEmptyChannelGroup() {
        Channels objectUnderTest = new Channels();

        assertThrows("Expected refresh to fail for empty channel group.",
                () -> objectUnderTest.refresh(),
                e -> e instanceof IllegalArgumentException);

    }

    @Test
    public void testRefreshReturnsNewInstanceForNonEmptyChannelGroup() {
        Channels objectUnderTest = new Channels();
        objectUnderTest.add(new EmbeddedChannel());
        ChannelGroup cg = objectUnderTest.refresh();
        assertNotEquals(cg, objectUnderTest.get());
    }
}
