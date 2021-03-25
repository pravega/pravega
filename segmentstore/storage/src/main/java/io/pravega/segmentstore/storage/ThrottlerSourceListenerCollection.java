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
package io.pravega.segmentstore.storage;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.Exceptions;
import java.util.ArrayList;
import java.util.HashSet;
import javax.annotation.concurrent.GuardedBy;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * A collection of {@link ThrottleSourceListener} objects.
 */
@Slf4j
public class ThrottlerSourceListenerCollection {
    @GuardedBy("listeners")
    private final HashSet<ThrottleSourceListener> listeners = new HashSet<>();

    @VisibleForTesting
    int getListenerCount() {
        synchronized (this.listeners) {
            return this.listeners.size();
        }
    }

    /**
     * Registers a new {@link ThrottleSourceListener}.
     *
     * @param listener The listener to register. This listener will be automatically unregistered when {@link #notifySourceChanged()}
     *                 is invoked and {@link ThrottleSourceListener#isClosed()} is true for it.
     */
    public void register(@NonNull ThrottleSourceListener listener) {
        if (listener.isClosed()) {
            log.warn("Attempted to register a closed ThrottleSourceListener ({}).", listener);
            return;
        }

        synchronized (this.listeners) {
            this.listeners.add(listener); // This is a Set, so we won't be adding the same listener twice.
        }
    }

    /**
     * Notifies all registered {@link ThrottleSourceListener} instances that something has changed.
     */
    public void notifySourceChanged() {
        ArrayList<ThrottleSourceListener> toNotify = new ArrayList<>();
        ArrayList<ThrottleSourceListener> toRemove = new ArrayList<>();
        synchronized (this.listeners) {
            for (ThrottleSourceListener l : this.listeners) {
                if (l.isClosed()) {
                    toRemove.add(l);
                } else {
                    toNotify.add(l);
                }
            }

            this.listeners.removeAll(toRemove);
        }

        for (ThrottleSourceListener l : toNotify) {
            try {
                l.notifyThrottleSourceChanged();
            } catch (Throwable ex) {
                if (Exceptions.mustRethrow(ex)) {
                    throw ex;
                }

                log.error("Error while notifying listener {}.", l, ex);
            }
        }
    }
}