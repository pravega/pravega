/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.health;

public interface Registry<T> {

    /**
     * Register some object of arbitrary type to the registry.
     *
     * @param object The object to register.
     */
    void register(T object);

    /**
     * Unregister some object of arbitrary type to the registry.
     *
     * @param object The object to unregister.
     */
    void unregister(T object);
}
