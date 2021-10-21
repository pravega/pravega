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
import io.pravega.client.stream.notifications.notifier.SegmentNotifier;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.concurrent.GuardedBy;
import lombok.Synchronized;

/**
 * Factory used to create different types of notifiers.
 * To add a new notifier add a method which returns a new Notifier object which internally implements
 * {@link io.pravega.client.stream.notifications.Observable}
 */
public class NotifierFactory {

    private final NotificationSystem system;
    @GuardedBy("$lock")
    private StateSynchronizer<ReaderGroupState> synchronizer;
    @GuardedBy("$lock")
    private SegmentNotifier segmentNotifier;
    @GuardedBy("$lock")
    private EndOfDataNotifier endOfDataNotifier;

    public NotifierFactory(final NotificationSystem notificationSystem,
                           final StateSynchronizer<ReaderGroupState> synchronizer) {
        this.system = notificationSystem;
        this.synchronizer = synchronizer;
    }

    @Synchronized
    public SegmentNotifier getSegmentNotifier(final ScheduledExecutorService executor) {
        if (segmentNotifier == null) {
            segmentNotifier = new SegmentNotifier(this.system, this.synchronizer, executor);
        }
        return segmentNotifier;
    }

    @Synchronized
    public EndOfDataNotifier getEndOfDataNotifier(final ScheduledExecutorService executor) {
        if (endOfDataNotifier == null) {
            endOfDataNotifier = new EndOfDataNotifier(this.system, this.synchronizer, executor);
        }
        return endOfDataNotifier;
    }

    // multiple such notifiers can be added.
}
