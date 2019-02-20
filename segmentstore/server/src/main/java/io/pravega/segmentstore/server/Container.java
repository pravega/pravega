/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server;

import com.google.common.util.concurrent.Service;

/**
 * Defines a Container that can encapsulate a runnable component.
 * Has the ability to Start and Stop processing at any given time.
 */
public interface Container extends Service, AutoCloseable {
    /**
     * Gets a value indicating the Id of this container.
     * @return The Id of this container.
     */
    int getId();

    /**
     * Gets a value indicating whether the Container is in an Offline state. When in such a state, even if state() == Service.State.RUNNING,
     * all operations invoked on it will fail with ContainerOfflineException.
     *
     * @return True if the Container is Offline, false if Online.
     */
    boolean isOffline();


    @Override
    void close();
}


