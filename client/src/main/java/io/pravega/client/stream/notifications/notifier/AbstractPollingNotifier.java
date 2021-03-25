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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.stream.impl.ReaderGroupState;
import io.pravega.client.stream.notifications.Listener;
import io.pravega.client.stream.notifications.Notification;
import io.pravega.client.stream.notifications.NotificationSystem;
import javax.annotation.concurrent.GuardedBy;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractPollingNotifier<T extends Notification> extends AbstractNotifier<T> {

    final StateSynchronizer<ReaderGroupState> synchronizer;
    @GuardedBy("$lock")
    private ScheduledFuture<?> pollingTaskFuture;
    private final AtomicBoolean pollingStarted = new AtomicBoolean();

    AbstractPollingNotifier(final NotificationSystem notifySystem, final ScheduledExecutorService executor,
                            final StateSynchronizer<ReaderGroupState> synchronizer) {
        super(notifySystem, executor);
        this.synchronizer = synchronizer;
    }

    @Override
    @Synchronized
    public void unregisterListener(final Listener<T> listener) {
        super.unregisterListener(listener);
        if (!notifySystem.isListenerPresent(getType())) {
            cancelScheduledTask();
            synchronizer.close();
        }
    }

    @Override
    @Synchronized
    public void unregisterAllListeners() {
        super.unregisterAllListeners();
        cancelScheduledTask();
        synchronizer.close();
    }

    void cancelScheduledTask() {
        log.debug("Cancel the scheduled task to check");
        if (pollingTaskFuture != null) {
            pollingTaskFuture.cancel(true);
        }
        pollingStarted.set(false);
    }

    void startPolling(final Runnable pollingTask, int pollingIntervalSeconds) {
        if (!pollingStarted.getAndSet(true)) { //schedule task only once
            pollingTaskFuture = executor.scheduleAtFixedRate(pollingTask, 0, pollingIntervalSeconds, TimeUnit.SECONDS);
        }
    }
}
