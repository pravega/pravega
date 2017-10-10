/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.notifications;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import io.pravega.client.stream.notifications.events.ScaleEvent;
import io.pravega.client.stream.notifications.notifier.ScaleEventNotifier;
import io.pravega.common.util.ReusableLatch;
import io.pravega.test.common.InlineExecutor;
import lombok.extern.slf4j.Slf4j;

@RunWith(MockitoJUnitRunner.class)
@Slf4j
public class ScaleNotifierTest {

    @Spy
    private NotificationSystem system = new NotificationSystem();
    @Spy
    private ScheduledExecutorService executor = new InlineExecutor();

    @Test
    public void scaleNotifierTest() throws Exception {
        AtomicBoolean listenerInvoked = new AtomicBoolean();
        ReusableLatch latch = new ReusableLatch();
        Supplier<ScaleEvent> s = () -> ScaleEvent.builder().numOfReaders(1).numOfSegments(2).build();
        Listener<ScaleEvent> listener1 = event -> {
            log.info("listener 1 invoked");
            listenerInvoked.set(true);
            latch.release();
        };
        Listener<ScaleEvent> listener2 = event -> {
        };

        ScaleEventNotifier notifier = new ScaleEventNotifier(system, s);
        notifier.addListener(listener1, executor);
        verify(executor, times(1)).scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));
        latch.await();
        assertTrue(listenerInvoked.get());

        notifier.addListener(listener2, executor);
        verify(executor, times(1)).scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));

        notifier.addListener(listener1, executor); //duplicate listener

        notifier.removeListeners();
        verify(system, times(1)).removeListeners(ScaleEvent.class);
    }
}
