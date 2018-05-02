/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.notifications;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.pravega.common.concurrent.ExecutorServiceHelpers;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.stream.impl.ReaderGroupState;
import io.pravega.client.stream.notifications.notifier.EndOfDataNotifier;
import io.pravega.common.util.ReusableLatch;
import io.pravega.test.common.InlineExecutor;
import lombok.extern.slf4j.Slf4j;

@RunWith(MockitoJUnitRunner.class)
@Slf4j
public class EndOfDataNotifierTest {

    @Spy
    private NotificationSystem system = new NotificationSystem();
    @Spy
    private ScheduledExecutorService executor = new InlineExecutor();
    @Mock
    private StateSynchronizer<ReaderGroupState> sync;
    @Mock
    private ReaderGroupState state;

    @BeforeClass
    public static void beforeClass() {
        System.setProperty("pravega.client.endOfDataNotification.poll.interval.seconds", String.valueOf(5));
    }

    @Test
    public void endOfStreamNotifierTest() throws Exception {
        AtomicBoolean listenerInvoked = new AtomicBoolean();
        ReusableLatch latch = new ReusableLatch();

        when(state.isEndOfData()).thenReturn(false).thenReturn(true);
        when(sync.getState()).thenReturn(state);

        Listener<EndOfDataNotification> listener1 = notification -> {
            log.info("listener 1 invoked");
            listenerInvoked.set(true);
            latch.release();
        };
        Listener<EndOfDataNotification> listener2 = notification -> {
        };

        EndOfDataNotifier notifier = new EndOfDataNotifier(system, sync, executor);
        notifier.registerListener(listener1);
        verify(executor, times(1)).scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));
        latch.await();
        verify(state, times(2)).isEndOfData();
        assertTrue(listenerInvoked.get());

        notifier.registerListener(listener2);
        verify(executor, times(1)).scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));

        notifier.unregisterAllListeners();
        verify(system, times(1)).removeListeners(EndOfDataNotification.class.getSimpleName());
    }

    @After
    public void cleanup() {
        ExecutorServiceHelpers.shutdown(executor);
    }
}
