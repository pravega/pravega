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
import io.pravega.client.stream.notifications.notifier.EndOfDataNotifier;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.test.common.InlineExecutor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

    @Test(timeout = 10000)
    public void endOfStreamNotifierTest() throws Exception {
        AtomicBoolean listenerInvoked = new AtomicBoolean();

        when(state.isEndOfData()).thenReturn(false).thenReturn(true);
        when(sync.getState()).thenReturn(state);

        Listener<EndOfDataNotification> listener1 = notification -> {
            log.info("listener 1 invoked");
            listenerInvoked.set(true);
        };
        Listener<EndOfDataNotification> listener2 = notification -> {
        };

        EndOfDataNotifier notifier = new EndOfDataNotifier(system, sync, executor);
        notifier.registerListener(listener1);
        verify(executor, times(1)).scheduleAtFixedRate(any(Runnable.class), eq(0L), anyLong(), any(TimeUnit.class));
        notifier.pollNow();
        verify(state, times(2)).isEndOfData();
        assertTrue(listenerInvoked.get());

        notifier.registerListener(listener2);
        verify(executor, times(1)).scheduleAtFixedRate(any(Runnable.class), eq(0L), anyLong(), any(TimeUnit.class));

        notifier.unregisterAllListeners();
        verify(system, times(1)).removeListeners(EndOfDataNotification.class.getSimpleName());
    }

    @Test(timeout = 10000)
    public void endOfStreamNotifierWithEmptyState() throws Exception {
        AtomicBoolean listenerInvoked = new AtomicBoolean();

        when(state.isEndOfData()).thenReturn(false).thenReturn(true);
        when(sync.getState()).thenReturn(null).thenReturn(state);

        Listener<EndOfDataNotification> listener1 = notification -> {
            log.info("listener 1 invoked");
            listenerInvoked.set(true);
        };

        EndOfDataNotifier notifier = new EndOfDataNotifier(system, sync, executor);
        notifier.registerListener(listener1);
        verify(executor, times(1)).scheduleAtFixedRate(any(Runnable.class), eq(0L), anyLong(), any(TimeUnit.class));
        notifier.pollNow();
        verify(state, times(1)).isEndOfData();
        notifier.pollNow();
        verify(state, times(2)).isEndOfData();
        assertTrue(listenerInvoked.get());
    }

    @After
    public void cleanup() {
        ExecutorServiceHelpers.shutdown(executor);
    }
}
