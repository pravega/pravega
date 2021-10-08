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

import static com.google.common.base.Preconditions.checkState;

import java.util.concurrent.ScheduledExecutorService;
import com.google.common.annotations.VisibleForTesting;
import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.stream.impl.ReaderGroupState;
import io.pravega.client.stream.notifications.Listener;
import io.pravega.client.stream.notifications.NotificationSystem;
import io.pravega.client.stream.notifications.SegmentNotification;
import javax.annotation.concurrent.GuardedBy;

import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SegmentNotifier extends AbstractPollingNotifier<SegmentNotification> {
    private static final int UPDATE_INTERVAL_SECONDS = Integer.parseInt(
            System.getProperty("pravega.client.segmentNotification.poll.interval.seconds", String.valueOf(120)));
    @GuardedBy("$lock")
    private int numberOfSegments = 0;

    public SegmentNotifier(final NotificationSystem notifySystem,
                           final StateSynchronizer<ReaderGroupState> synchronizer,
                           final ScheduledExecutorService executor) {
        super(notifySystem, executor, synchronizer);
    }
    
    /**
     * Invokes the periodic processing now in the current thread.
     */
    @VisibleForTesting
    public void pollNow() {
        checkAndTriggerSegmentNotification();
    }

    @Override
    @Synchronized
    public void registerListener(final Listener<SegmentNotification> listener) {
        notifySystem.addListeners(getType(), listener, this.executor);
        //periodically fetch the segment count.
        startPolling(this::checkAndTriggerSegmentNotification, UPDATE_INTERVAL_SECONDS);
    }

    @Override
    public String getType() {
        return SegmentNotification.class.getSimpleName();
    }

    private void checkAndTriggerSegmentNotification() {
        this.synchronizer.fetchUpdates();
        ReaderGroupState state = this.synchronizer.getState();
        if (state == null ) {
            log.warn("Current state of StateSynchronizer {} is null, will try again.", synchronizer);
        } else {
            int newNumberOfSegments = state.getNumberOfSegments();
            log.debug("Number of segments in {} is {}", synchronizer, newNumberOfSegments);
            checkState(newNumberOfSegments > 0, "Number of segments cannot be zero");

            //Trigger a notification with the initial number of segments.
            //Subsequent notifications are triggered only if there is a change in the number of segments.
            if (this.numberOfSegments != newNumberOfSegments) {
                this.numberOfSegments = newNumberOfSegments;
                SegmentNotification notification = SegmentNotification.builder().numOfSegments(state.getNumberOfSegments())
                                                                      .numOfReaders(state.getOnlineReaders().size())
                                                                      .build();
                notifySystem.notify(notification);
            }
        }
    }
}
