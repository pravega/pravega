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

public interface Observable<Type> {
    //TODO: shrids: add executor.

    /**
     * Add listener for event type T.
     * @param listener This is the listener which will be invoked incase of an Event.
     */
    void addListener(Listener<Type> listener);

    /**
     * Remove a listener.
     * @param listener the listener which needs to be removed.
     */
    void removeListener(Listener<Type> listener);

    //TODO: shrids removeListener of all types

    /**
     * Get the event type
     * @return
     */
    Class<Type> getType();
}
