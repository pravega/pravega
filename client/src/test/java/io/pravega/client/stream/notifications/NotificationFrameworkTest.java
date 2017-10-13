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
import io.pravega.client.stream.notifications.events.ScaleEvent;
import io.pravega.client.stream.notifications.notifier.ScaleEventNotifier;
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
    public void scaleEventTest() {
        final AtomicBoolean scaleEventReceived = new AtomicBoolean(false);

        //Application can subscribe to scale events in the following way.
        Observable<ScaleEvent> notifier = new ScaleEventNotifier(notificationSystem, () -> sync);
        notifier.registerListener(scaleEvent -> {
            int numReader = scaleEvent.getNumOfReaders();
            int segments = scaleEvent.getNumOfSegments();
            if (numReader < segments) {
                System.out.println("Scale up number of readers based on my capacity");
            } else {
                System.out.println("More readers available time to shut down some");
            }
            scaleEventReceived.set(true);
        }, executor);

        //Trigger notification.
        notificationSystem.notify(ScaleEvent.builder().numOfSegments(3).numOfReaders(4).build());
        assertTrue("Scale Event notification received", scaleEventReceived.get());

        scaleEventReceived.set(false);

        //Trigger notification.
        notificationSystem.notify(ScaleEvent.builder().numOfSegments(5).numOfReaders(4).build());
        assertTrue("Scale Event notification received", scaleEventReceived.get());

        scaleEventReceived.set(false);

        notifier.unregisterListeners();
        //Trigger notification.
        notificationSystem.notify(ScaleEvent.builder().numOfSegments(5).numOfReaders(4).build());
        Assert.assertFalse("Scale Event notification should not be received", scaleEventReceived.get());

        final AtomicBoolean listener1Invoked = new AtomicBoolean();
        final AtomicBoolean listener2Invoked = new AtomicBoolean();

        Listener<ScaleEvent> listener1 = event -> listener1Invoked.set(true);
        Listener<ScaleEvent> listener2 = event -> listener2Invoked.set(true);
        notifier.registerListener(listener1, executor);
        notifier.registerListener(listener2, executor);

        //Trigger notification.
        notificationSystem.notify(ScaleEvent.builder().numOfSegments(5).numOfReaders(4).build());
        assertTrue("Scale Event notification not received on listener 1", listener1Invoked.get());
        assertTrue("Scale Event notification not received on listener 2", listener2Invoked.get());

        notifier.unregisterListener(listener1);
        notifier.unregisterListener(listener2);

        listener1Invoked.set(false);
        listener2Invoked.set(false);
        //Trigger notification.
        notificationSystem.notify(ScaleEvent.builder().numOfSegments(5).numOfReaders(4).build());
        Assert.assertFalse("Scale Event notification received on listener 1", listener1Invoked.get());
        Assert.assertFalse("Scale Event notification received on listener 2", listener2Invoked.get());
    }

    @Test
    public void multipleEventTest() {
        final AtomicBoolean scaleEventListenerInvoked = new AtomicBoolean();
        final AtomicBoolean customEventListenerInvoked = new AtomicBoolean();

        Observable<ScaleEvent> scaleNotifier = new ScaleEventNotifier(notificationSystem, () -> sync);
        Listener<ScaleEvent> scaleEventListener = event -> scaleEventListenerInvoked.set(true);
        scaleNotifier.registerListener(scaleEventListener, executor);

        Observable<CustomEvent> customEventNotifier = new CustomEventNotifier(notificationSystem);
        Listener<CustomEvent> customEventListener = event -> customEventListenerInvoked.set(true);
        customEventNotifier.registerListener(customEventListener, executor);

        //trigger notifications
        notificationSystem.notify(ScaleEvent.builder().numOfSegments(5).numOfReaders(4).build());
        assertTrue(scaleEventListenerInvoked.get());
        assertFalse(customEventListenerInvoked.get());

        scaleEventListenerInvoked.set(false);

        //trigger notifications
        notificationSystem.notify(CustomEvent.builder().build());
        assertFalse(scaleEventListenerInvoked.get());
        assertTrue(customEventListenerInvoked.get());

        customEventNotifier.unregisterListeners();
        customEventListenerInvoked.set(false);

        //trigger notifications
        notificationSystem.notify(CustomEvent.builder().build());
        assertFalse(scaleEventListenerInvoked.get());
        assertFalse(customEventListenerInvoked.get());
    }

    @Test
    public void notifierFactoryTest() {
        final AtomicBoolean scaleEventReceived = new AtomicBoolean(false);

        //Application can subscribe to scale events in the following way.
        final Observable<ScaleEvent> notifier = new ScaleEventNotifier(notificationSystem, () -> sync);
        notifier.registerListener(scaleEvent -> {
            int numReader = scaleEvent.getNumOfReaders();
            int segments = scaleEvent.getNumOfSegments();
            if (numReader < segments) {
                System.out.println("Scale up number of readers based on my capacity");
            } else {
                System.out.println("More readers available time to shut down some");
            }
            scaleEventReceived.set(true);
        }, executor);

        //Trigger notification.
        notificationSystem.notify(ScaleEvent.builder().numOfSegments(3).numOfReaders(4).build());
        assertTrue("Scale Event notification received", scaleEventReceived.get());

        scaleEventReceived.set(false);

        //Trigger notification.
        notificationSystem.notify(ScaleEvent.builder().numOfSegments(5).numOfReaders(4).build());
        assertTrue("Scale Event notification received", scaleEventReceived.get());

        scaleEventReceived.set(false);

        notifier.unregisterListeners();
        //Trigger notification.
        notificationSystem.notify(ScaleEvent.builder().numOfSegments(5).numOfReaders(4).build());
        Assert.assertFalse("Scale Event notification should not be received", scaleEventReceived.get());

        final AtomicBoolean listener1Invoked = new AtomicBoolean();
        final AtomicBoolean listener2Invoked = new AtomicBoolean();

        Listener<ScaleEvent> listener1 = event -> listener1Invoked.set(true);
        Listener<ScaleEvent> listener2 = event -> listener2Invoked.set(true);
        notifier.registerListener(listener1, executor);
        notifier.registerListener(listener2, executor);

        //Trigger notification.
        notificationSystem.notify(ScaleEvent.builder().numOfSegments(5).numOfReaders(4).build());
        assertTrue("Scale Event notification not received on listener 1", listener1Invoked.get());
        assertTrue("Scale Event notification not received on listener 2", listener2Invoked.get());

        notifier.unregisterListener(listener1);
        notifier.unregisterListener(listener2);

        listener1Invoked.set(false);
        listener2Invoked.set(false);
        //Trigger notification.
        notificationSystem.notify(ScaleEvent.builder().numOfSegments(5).numOfReaders(4).build());
        Assert.assertFalse("Scale Event notification received on listener 1", listener1Invoked.get());
        Assert.assertFalse("Scale Event notification received on listener 2", listener2Invoked.get());
    }
}

