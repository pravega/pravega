/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.notifications;

import java.util.concurrent.ScheduledExecutorService;

/**
 * This represents an observable notification.
 * @param <T>
 */
public interface Observable<T> {
    /**
     * Register listener for event type T. Multiple listeners can be added for the same type.
     * @param listener This is the listener which will be invoked incase of an Event.
     * @param executor This is the executor on which the listener will be executed.
     */
    void registerListener(final Listener<T> listener, final ScheduledExecutorService executor);

    /**
     * Remove a listener.
     * @param listener the listener which needs to be removed.
     */
    void unregisterListener(final Listener<T> listener);

    /**
     * Remove all listeners for a given type.
     */
    void unregisterListeners();

    /**
     * Get the event type.
     * @return Event type.
     */
    String getType();
}
