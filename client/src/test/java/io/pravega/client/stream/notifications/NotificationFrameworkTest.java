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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.stream.impl.ReaderGroupImpl;
import io.pravega.client.stream.impl.ReaderGroupState;
import io.pravega.client.stream.notifications.events.SegmentEvent;
import io.pravega.client.stream.notifications.notifier.SegmentEventNotifier;
import io.pravega.test.common.InlineExecutor;

@RunWith(MockitoJUnitRunner.class)
public class NotificationFrameworkTest {

    private final ScheduledExecutorService executor = new InlineExecutor();
    @Spy
    private final ReaderGroupImpl readerGroup = new ReaderGroupImpl("testScope", "rg1", null, null, null,
            null, null, null);
    private final NotificationSystem notificationSystem = readerGroup.getNotificationSystem();
    @Mock
    private StateSynchronizer<ReaderGroupState> sync;

    @Test
    public void notificationFrameworkTest() {
        final AtomicBoolean segEventReceived = new AtomicBoolean(false);

        //Application can subscribe to segment events in the following way.
        Observable<SegmentEvent> notifier = new SegmentEventNotifier(notificationSystem, () -> sync, executor);
        notifier.registerListener(segmentEvent -> {
            int numReader = segmentEvent.getNumOfReaders();
            int segments = segmentEvent.getNumOfSegments();
            if (numReader < segments) {
                System.out.println("Scale up number of readers based on my capacity");
            } else {
                System.out.println("More readers available time to shut down some");
            }
            segEventReceived.set(true);
        });

        //Trigger notification.
        notificationSystem.notify(SegmentEvent.builder().numOfSegments(3).numOfReaders(4).build());
        assertTrue("Segment Event notification received", segEventReceived.get());

        segEventReceived.set(false);

        //Trigger notification.
        notificationSystem.notify(SegmentEvent.builder().numOfSegments(5).numOfReaders(4).build());
        assertTrue("Segment Event notification received", segEventReceived.get());

        segEventReceived.set(false);

        notifier.unregisterAllListeners();
        //Trigger notification.
        notificationSystem.notify(SegmentEvent.builder().numOfSegments(5).numOfReaders(4).build());
        Assert.assertFalse("Segment Event notification should not be received", segEventReceived.get());

        final AtomicBoolean listener1Invoked = new AtomicBoolean();
        final AtomicBoolean listener2Invoked = new AtomicBoolean();

        Listener<SegmentEvent> listener1 = event -> listener1Invoked.set(true);
        Listener<SegmentEvent> listener2 = event -> listener2Invoked.set(true);
        notifier.registerListener(listener1);
        notifier.registerListener(listener2);

        //Trigger notification.
        notificationSystem.notify(SegmentEvent.builder().numOfSegments(5).numOfReaders(4).build());
        assertTrue("Segment Event notification not received on listener 1", listener1Invoked.get());
        assertTrue("Segment Event notification not received on listener 2", listener2Invoked.get());

        notifier.unregisterListener(listener1);
        notifier.unregisterListener(listener2);

        listener1Invoked.set(false);
        listener2Invoked.set(false);
        //Trigger notification.
        notificationSystem.notify(SegmentEvent.builder().numOfSegments(5).numOfReaders(4).build());
        Assert.assertFalse("Segment Event notification received on listener 1", listener1Invoked.get());
        Assert.assertFalse("Segment Event notification received on listener 2", listener2Invoked.get());
    }

    @Test
    public void multipleEventTest() {
        final AtomicBoolean segEventListenerInvoked = new AtomicBoolean();
        final AtomicBoolean customEventListenerInvoked = new AtomicBoolean();

        Observable<SegmentEvent> segmentNotifier = new SegmentEventNotifier(notificationSystem, () -> sync, executor);
        Listener<SegmentEvent> segmentEventListener = event -> segEventListenerInvoked.set(true);
        segmentNotifier.registerListener(segmentEventListener);

        Observable<CustomEvent> customEventNotifier = new CustomEventNotifier(notificationSystem, executor);
        Listener<CustomEvent> customEventListener = event -> customEventListenerInvoked.set(true);
        customEventNotifier.registerListener(customEventListener);

        //trigger notifications
        notificationSystem.notify(SegmentEvent.builder().numOfSegments(5).numOfReaders(4).build());
        assertTrue(segEventListenerInvoked.get());
        assertFalse(customEventListenerInvoked.get());

        segEventListenerInvoked.set(false);

        //trigger notifications
        notificationSystem.notify(CustomEvent.builder().build());
        assertFalse(segEventListenerInvoked.get());
        assertTrue(customEventListenerInvoked.get());

        customEventNotifier.unregisterAllListeners();
        customEventListenerInvoked.set(false);

        //trigger notifications
        notificationSystem.notify(CustomEvent.builder().build());
        assertFalse(segEventListenerInvoked.get());
        assertFalse(customEventListenerInvoked.get());
    }

    @Test
    public void notifierFactoryTest() {
        final AtomicBoolean segmentEventReceived = new AtomicBoolean(false);

        //Application can subscribe to segment events in the following way.
        final Observable<SegmentEvent> notifier = new SegmentEventNotifier(notificationSystem, () -> sync, executor);
        notifier.registerListener(segmentEvent -> {
            int numReader = segmentEvent.getNumOfReaders();
            int segments = segmentEvent.getNumOfSegments();
            if (numReader < segments) {
                System.out.println("Scale up number of readers based on my capacity");
            } else {
                System.out.println("More readers available time to shut down some");
            }
            segmentEventReceived.set(true);
        });

        //Trigger notification.
        notificationSystem.notify(SegmentEvent.builder().numOfSegments(3).numOfReaders(4).build());
        assertTrue("Segment Event notification received", segmentEventReceived.get());

        segmentEventReceived.set(false);

        //Trigger notification.
        notificationSystem.notify(SegmentEvent.builder().numOfSegments(5).numOfReaders(4).build());
        assertTrue("Segment Event notification received", segmentEventReceived.get());

        segmentEventReceived.set(false);

        notifier.unregisterAllListeners();
        //Trigger notification.
        notificationSystem.notify(SegmentEvent.builder().numOfSegments(5).numOfReaders(4).build());
        Assert.assertFalse("Segment Event notification should not be received", segmentEventReceived.get());

        final AtomicBoolean listener1Invoked = new AtomicBoolean();
        final AtomicBoolean listener2Invoked = new AtomicBoolean();

        Listener<SegmentEvent> listener1 = event -> listener1Invoked.set(true);
        Listener<SegmentEvent> listener2 = event -> listener2Invoked.set(true);
        notifier.registerListener(listener1);
        notifier.registerListener(listener2);

        //Trigger notification.
        notificationSystem.notify(SegmentEvent.builder().numOfSegments(5).numOfReaders(4).build());
        assertTrue("Segment Event notification not received on listener 1", listener1Invoked.get());
        assertTrue("Segment Event notification not received on listener 2", listener2Invoked.get());

        notifier.unregisterListener(listener1);
        notifier.unregisterListener(listener2);

        listener1Invoked.set(false);
        listener2Invoked.set(false);
        //Trigger notification.
        notificationSystem.notify(SegmentEvent.builder().numOfSegments(5).numOfReaders(4).build());
        Assert.assertFalse("Segment Event notification received on listener 1", listener1Invoked.get());
        Assert.assertFalse("Segment Event notification received on listener 2", listener2Invoked.get());
    }
}

