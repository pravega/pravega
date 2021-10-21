/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.client.stream.notifications;

import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.stream.impl.ReaderGroupState;
import io.pravega.client.stream.notifications.notifier.SegmentNotifier;
import io.pravega.test.common.InlineExecutor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class NotificationFrameworkTest {

    private final ScheduledExecutorService executor = new InlineExecutor();

    private final NotificationSystem notificationSystem = new NotificationSystem();
    @Mock
    private StateSynchronizer<ReaderGroupState> sync;

    @Test
    public void notificationFrameworkTest() {
        final AtomicBoolean segNotificationReceived = new AtomicBoolean(false);

        //Application can subscribe to segment notifications in the following way.
        Observable<SegmentNotification> notifier = new SegmentNotifier(notificationSystem, sync, executor);
        notifier.registerListener(segmentNotification -> {
            int numReader = segmentNotification.getNumOfReaders();
            int segments = segmentNotification.getNumOfSegments();
            if (numReader < segments) {
                System.out.println("Scale up number of readers based on my capacity");
            } else {
                System.out.println("More readers available time to shut down some");
            }
            segNotificationReceived.set(true);
        });

        //Trigger notification.
        notificationSystem.notify(SegmentNotification.builder().numOfSegments(3).numOfReaders(4).build());
        assertTrue("Segment Notification notification received", segNotificationReceived.get());

        segNotificationReceived.set(false);

        //Trigger notification.
        notificationSystem.notify(SegmentNotification.builder().numOfSegments(5).numOfReaders(4).build());
        assertTrue("Segment Notification notification received", segNotificationReceived.get());

        segNotificationReceived.set(false);

        notifier.unregisterAllListeners();
        //Trigger notification.
        notificationSystem.notify(SegmentNotification.builder().numOfSegments(5).numOfReaders(4).build());
        Assert.assertFalse("Segment Notification notification should not be received", segNotificationReceived.get());

        final AtomicBoolean listener1Invoked = new AtomicBoolean();
        final AtomicBoolean listener2Invoked = new AtomicBoolean();

        Listener<SegmentNotification> listener1 = e -> listener1Invoked.set(true);
        Listener<SegmentNotification> listener2 = e -> listener2Invoked.set(true);
        notifier.registerListener(listener1);
        notifier.registerListener(listener2);

        //Trigger notification.
        notificationSystem.notify(SegmentNotification.builder().numOfSegments(5).numOfReaders(4).build());
        assertTrue("Segment Notification notification not received on listener 1", listener1Invoked.get());
        assertTrue("Segment Notification notification not received on listener 2", listener2Invoked.get());

        notifier.unregisterListener(listener1);
        notifier.unregisterListener(listener2);

        listener1Invoked.set(false);
        listener2Invoked.set(false);
        //Trigger notification.
        notificationSystem.notify(SegmentNotification.builder().numOfSegments(5).numOfReaders(4).build());
        Assert.assertFalse("Segment Notification notification received on listener 1", listener1Invoked.get());
        Assert.assertFalse("Segment Notification notification received on listener 2", listener2Invoked.get());
    }

    @Test
    public void multipleNotificationTest() {
        final AtomicBoolean segListenerInvoked = new AtomicBoolean();
        final AtomicBoolean customListenerInvoked = new AtomicBoolean();

        Observable<SegmentNotification> segmentNotifier = new SegmentNotifier(notificationSystem, sync, executor);
        Listener<SegmentNotification> segmentListener = e -> segListenerInvoked.set(true);
        segmentNotifier.registerListener(segmentListener);

        Observable<CustomNotification> customNotifier = new CustomNotifier(notificationSystem, executor);
        Listener<CustomNotification> customListener = e -> customListenerInvoked.set(true);
        customNotifier.registerListener(customListener);

        //trigger notifications
        notificationSystem.notify(SegmentNotification.builder().numOfSegments(5).numOfReaders(4).build());
        assertTrue(segListenerInvoked.get());
        assertFalse(customListenerInvoked.get());

        segListenerInvoked.set(false);

        //trigger notifications
        notificationSystem.notify(CustomNotification.builder().build());
        assertFalse(segListenerInvoked.get());
        assertTrue(customListenerInvoked.get());

        customNotifier.unregisterAllListeners();
        customListenerInvoked.set(false);

        //trigger notifications
        notificationSystem.notify(CustomNotification.builder().build());
        assertFalse(segListenerInvoked.get());
        assertFalse(customListenerInvoked.get());
    }

    @Test
    public void notifierFactoryTest() {
        final AtomicBoolean segmentNotificationReceived = new AtomicBoolean(false);

        //Application can subscribe to segment notifications in the following way.
        final Observable<SegmentNotification> notifier = new SegmentNotifier(notificationSystem, sync, executor);
        notifier.registerListener(segmentNotification -> {
            int numReader = segmentNotification.getNumOfReaders();
            int segments = segmentNotification.getNumOfSegments();
            if (numReader < segments) {
                System.out.println("Scale up number of readers based on my capacity");
            } else {
                System.out.println("More readers available time to shut down some");
            }
            segmentNotificationReceived.set(true);
        });

        //Trigger notification.
        notificationSystem.notify(SegmentNotification.builder().numOfSegments(3).numOfReaders(4).build());
        assertTrue("Segment Notification notification received", segmentNotificationReceived.get());

        segmentNotificationReceived.set(false);

        //Trigger notification.
        notificationSystem.notify(SegmentNotification.builder().numOfSegments(5).numOfReaders(4).build());
        assertTrue("Segment Notification notification received", segmentNotificationReceived.get());

        segmentNotificationReceived.set(false);

        notifier.unregisterAllListeners();
        //Trigger notification.
        notificationSystem.notify(SegmentNotification.builder().numOfSegments(5).numOfReaders(4).build());
        Assert.assertFalse("Segment Notification notification should not be received", segmentNotificationReceived.get());

        final AtomicBoolean listener1Invoked = new AtomicBoolean();
        final AtomicBoolean listener2Invoked = new AtomicBoolean();

        Listener<SegmentNotification> listener1 = e -> listener1Invoked.set(true);
        Listener<SegmentNotification> listener2 = e -> listener2Invoked.set(true);
        notifier.registerListener(listener1);
        notifier.registerListener(listener2);

        //Trigger notification.
        notificationSystem.notify(SegmentNotification.builder().numOfSegments(5).numOfReaders(4).build());
        assertTrue("Segment Notification notification not received on listener 1", listener1Invoked.get());
        assertTrue("Segment Notification notification not received on listener 2", listener2Invoked.get());

        notifier.unregisterListener(listener1);
        notifier.unregisterListener(listener2);

        listener1Invoked.set(false);
        listener2Invoked.set(false);
        //Trigger notification.
        notificationSystem.notify(SegmentNotification.builder().numOfSegments(5).numOfReaders(4).build());
        Assert.assertFalse("Segment Notification notification received on listener 1", listener1Invoked.get());
        Assert.assertFalse("Segment Notification notification received on listener 2", listener2Invoked.get());
    }
}

