/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage;

/**
 * Defines a listener that will be notified every time the state of the a Throttling Source changed.
 */
public interface ThrottleSourceListener {
    /**
     * Notifies this {@link ThrottleSourceListener} the state of the Throttling Source has changed.
     */
    void notifyThrottleSourceChanged();

    /**
     * Gets a value indicating whether this {@link ThrottleSourceListener} is closed and should be unregistered.
     *
     * @return True if need to be unregistered (no further notifications will be sent), false otherwise.
     */
    boolean isClosed();
}
