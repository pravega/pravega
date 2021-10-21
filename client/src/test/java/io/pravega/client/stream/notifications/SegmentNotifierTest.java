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
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.test.common.InlineExecutor;
import java.util.HashSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
@Slf4j
public class SegmentNotifierTest {

    @Spy
    private NotificationSystem system = new NotificationSystem();
    @Spy
    private ScheduledExecutorService executor = new InlineExecutor();
    @Mock
    private StateSynchronizer<ReaderGroupState> sync;
    @Mock
    private ReaderGroupState state;

    @Test(timeout = 5000)
    public void segmentNotifierTest() throws Exception {
        AtomicBoolean listenerInvoked = new AtomicBoolean();
        AtomicInteger segmentCount = new AtomicInteger(0);

        when(state.getOnlineReaders()).thenReturn(new HashSet<>(singletonList("reader1")));
        when(state.getNumberOfSegments()).thenReturn(1, 1, 2 ).thenReturn(2);
        when(sync.getState()).thenReturn(state);

        Listener<SegmentNotification> listener1 = e -> {
            log.info("listener 1 invoked");
            listenerInvoked.set(true);
            segmentCount.set(e.getNumOfSegments());

        };
        Listener<SegmentNotification> listener2 = e -> {
        };

        SegmentNotifier notifier = new SegmentNotifier(system, sync, executor);
        notifier.registerListener(listener1);
        verify(executor, times(1)).scheduleAtFixedRate(any(Runnable.class), eq(0L), anyLong(), any(TimeUnit.class));
        assertTrue(listenerInvoked.get());
        assertEquals(1, segmentCount.get());
        listenerInvoked.set(false);
        notifier.pollNow();
        assertTrue(listenerInvoked.get());
        assertEquals(2, segmentCount.get());

        notifier.registerListener(listener2);
        verify(executor, times(1)).scheduleAtFixedRate(any(Runnable.class), eq(0L), anyLong(), any(TimeUnit.class));

        notifier.registerListener(listener1); //duplicate listener

        notifier.unregisterAllListeners();
        verify(system, times(1)).removeListeners(SegmentNotification.class.getSimpleName());
    }

    @Test(timeout = 5000)
    public void segmentNotifierTestWithEmptyState() throws Exception {
        AtomicBoolean listenerInvoked = new AtomicBoolean();
        AtomicInteger segmentCount = new AtomicInteger(0);

        when(state.getOnlineReaders()).thenReturn(new HashSet<>(singletonList("reader1")));
        when(state.getNumberOfSegments()).thenReturn(1, 1, 2 ).thenReturn(2);
        // simulate a null being returned.
        when(sync.getState()).thenReturn(null).thenReturn(state);

        Listener<SegmentNotification> listener1 = e -> {
            log.info("listener 1 invoked");
            listenerInvoked.set(true);
            segmentCount.set(e.getNumOfSegments());

        };
        SegmentNotifier notifier = new SegmentNotifier(system, sync, executor);
        notifier.registerListener(listener1);
        verify(executor, times(1)).scheduleAtFixedRate(any(Runnable.class), eq(0L), anyLong(), any(TimeUnit.class));
        notifier.pollNow();
        assertTrue(listenerInvoked.get());
        assertEquals(1, segmentCount.get());
    }

    @After
    public void cleanup() {
        ExecutorServiceHelpers.shutdown(executor);
    }
}
