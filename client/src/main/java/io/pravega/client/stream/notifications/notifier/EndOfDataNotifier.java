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
package io.pravega.client.stream.notifications.notifier;

import java.util.concurrent.ScheduledExecutorService;
import com.google.common.annotations.VisibleForTesting;
import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.stream.impl.ReaderGroupState;
import io.pravega.client.stream.notifications.EndOfDataNotification;
import io.pravega.client.stream.notifications.Listener;
import io.pravega.client.stream.notifications.NotificationSystem;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EndOfDataNotifier extends AbstractPollingNotifier<EndOfDataNotification> {
    private static final int UPDATE_INTERVAL_SECONDS = Integer.parseInt(
            System.getProperty("pravega.client.endOfDataNotification.poll.interval.seconds", String.valueOf(120)));

    public EndOfDataNotifier(final NotificationSystem notifySystem,
                             final StateSynchronizer<ReaderGroupState> synchronizer,
                             final ScheduledExecutorService executor) {
        super(notifySystem, executor, synchronizer);
    }
    
    /**
     * Invokes the periodic processing now in the current thread.
     */
    @VisibleForTesting
    public void pollNow() {
        this.checkAndTriggerEndOfStreamNotification();
    }

    @Override
    @Synchronized
    public void registerListener(final Listener<EndOfDataNotification> listener) {
        notifySystem.addListeners(getType(), listener, this.executor);
        //periodically check the for end of stream.
        startPolling(this::checkAndTriggerEndOfStreamNotification, UPDATE_INTERVAL_SECONDS);
    }

    @Override
    public String getType() {
        return EndOfDataNotification.class.getSimpleName();
    }

    private void checkAndTriggerEndOfStreamNotification() {
        this.synchronizer.fetchUpdates();
        ReaderGroupState state = this.synchronizer.getState();
        if (state == null) {
            log.warn("Current state of StateSynchronizer {} is null, will try again.", synchronizer);
        } else if (state.isEndOfData()) {
            notifySystem.notify(new EndOfDataNotification());
        }
    }
}
