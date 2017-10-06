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

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;

import io.pravega.client.stream.notifications.events.CustomEvent;
import io.pravega.client.stream.notifications.events.ScaleEvent;
import io.pravega.client.stream.notifications.notifier.CustomEventNotifier;
import io.pravega.client.stream.notifications.notifier.ScaleEventNotifier;

public class NotificationFrameworkTest {

    NotificationSystem readerGroup = new NotificationSystem();

    @Test
    public void scaleEventTest() {
        final AtomicBoolean scaleEventReceived = new AtomicBoolean(false);

        //Application can subscribe to scale events in the following way.
        ScaleEventNotifier notifier = new ScaleEventNotifier(readerGroup);
        notifier.addListener(scaleEvent -> {
            int numReader = scaleEvent.getNumOfReaders();
            int segments = scaleEvent.getNumOfSegments();
            if (numReader < segments) {
                System.out.println("Scale up number of readers based on my capacity");
            } else {
                System.out.println("More readers available time to shut down some");
            }
            scaleEventReceived.set(true);
        });

        //Trigger notification.
        readerGroup.notify(ScaleEvent.builder().numOfSegments(3).numOfReaders(4).build());
        assertTrue("Scale Event notification received", scaleEventReceived.get());

        scaleEventReceived.set(false);

        //Trigger notification.
        readerGroup.notify(ScaleEvent.builder().numOfSegments(5).numOfReaders(4).build());
        assertTrue("Scale Event notification received", scaleEventReceived.get());

        scaleEventReceived.set(false);

        notifier.removeListener();
        //Trigger notification.
        readerGroup.notify(ScaleEvent.builder().numOfSegments(5).numOfReaders(4).build());
        Assert.assertFalse("Scale Event notification should not be received", scaleEventReceived.get());

        final AtomicBoolean listener1Invoked = new AtomicBoolean();
        final AtomicBoolean listener2Invoked = new AtomicBoolean();

        Listener<ScaleEvent> listener1 = event -> listener1Invoked.set(true);
        Listener<ScaleEvent> listener2 = event -> listener2Invoked.set(true);
        notifier.addListener(listener1);
        notifier.addListener(listener2);

        //Trigger notification.
        readerGroup.notify(ScaleEvent.builder().numOfSegments(5).numOfReaders(4).build());
        assertTrue("Scale Event notification not received on listener 1", listener1Invoked.get());
        assertTrue("Scale Event notification not received on listener 2", listener2Invoked.get());

        readerGroup.removeListeners(listener1);
        readerGroup.removeListeners(listener2);

        listener1Invoked.set(false);
        listener2Invoked.set(false);
        //Trigger notification.
        readerGroup.notify(ScaleEvent.builder().numOfSegments(5).numOfReaders(4).build());
        Assert.assertFalse("Scale Event notification received on listener 1", listener1Invoked.get());
        Assert.assertFalse("Scale Event notification received on listener 2", listener2Invoked.get());
    }

    @Test
    public void multipleEventTest() {
        final AtomicBoolean scaleEventListenerInvoked = new AtomicBoolean();
        final AtomicBoolean customEventListenerInvoked = new AtomicBoolean();

        ScaleEventNotifier scaleNotifier = new ScaleEventNotifier(readerGroup);
        Listener<ScaleEvent> scaleEventListener = event -> scaleEventListenerInvoked.set(true);
        scaleNotifier.addListener(scaleEventListener);

        CustomEventNotifier customEventNotifier = new CustomEventNotifier(readerGroup);
        Listener<CustomEvent> customEventListener = event -> customEventListenerInvoked.set(true);
        customEventNotifier.addListener(customEventListener);

        //trigger notifications
        readerGroup.notify(ScaleEvent.builder().numOfSegments(5).numOfReaders(4).build());
        assertTrue(scaleEventListenerInvoked.get());
        assertFalse(customEventListenerInvoked.get());

        scaleEventListenerInvoked.set(false);

        //trigger notifications
        readerGroup.notify(CustomEvent.builder().build());
        assertFalse(scaleEventListenerInvoked.get());
        assertTrue(customEventListenerInvoked.get());

        customEventNotifier.removeListener();
        customEventListenerInvoked.set(false);

        //trigger notifications
        readerGroup.notify(CustomEvent.builder().build());
        assertFalse(scaleEventListenerInvoked.get());
        assertFalse(customEventListenerInvoked.get());
    }

    @Test
    public void notifierFactoryTest() {
        final AtomicBoolean scaleEventReceived = new AtomicBoolean(false);

        final NotifierFactory factory = readerGroup.getNotifierFactory();

        //Application can subscribe to scale events in the following way.
        final ScaleEventNotifier notifier = factory.getScaleNotifier();
        notifier.addListener(scaleEvent -> {
            int numReader = scaleEvent.getNumOfReaders();
            int segments = scaleEvent.getNumOfSegments();
            if (numReader < segments) {
                System.out.println("Scale up number of readers based on my capacity");
            } else {
                System.out.println("More readers available time to shut down some");
            }
            scaleEventReceived.set(true);
        });

        //Trigger notification.
        readerGroup.notify(ScaleEvent.builder().numOfSegments(3).numOfReaders(4).build());
        assertTrue("Scale Event notification received", scaleEventReceived.get());

        scaleEventReceived.set(false);

        //Trigger notification.
        readerGroup.notify(ScaleEvent.builder().numOfSegments(5).numOfReaders(4).build());
        assertTrue("Scale Event notification received", scaleEventReceived.get());

        scaleEventReceived.set(false);

        notifier.removeListener();
        //Trigger notification.
        readerGroup.notify(ScaleEvent.builder().numOfSegments(5).numOfReaders(4).build());
        Assert.assertFalse("Scale Event notification should not be received", scaleEventReceived.get());

        final AtomicBoolean listener1Invoked = new AtomicBoolean();
        final AtomicBoolean listener2Invoked = new AtomicBoolean();

        Listener<ScaleEvent> listener1 = event -> listener1Invoked.set(true);
        Listener<ScaleEvent> listener2 = event -> listener2Invoked.set(true);
        notifier.addListener(listener1);
        notifier.addListener(listener2);

        //Trigger notification.
        readerGroup.notify(ScaleEvent.builder().numOfSegments(5).numOfReaders(4).build());
        assertTrue("Scale Event notification not received on listener 1", listener1Invoked.get());
        assertTrue("Scale Event notification not received on listener 2", listener2Invoked.get());

        readerGroup.removeListeners(listener1);
        readerGroup.removeListeners(listener2);

        listener1Invoked.set(false);
        listener2Invoked.set(false);
        //Trigger notification.
        readerGroup.notify(ScaleEvent.builder().numOfSegments(5).numOfReaders(4).build());
        Assert.assertFalse("Scale Event notification received on listener 1", listener1Invoked.get());
        Assert.assertFalse("Scale Event notification received on listener 2", listener2Invoked.get());
    }
}

