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

import java.util.Optional;

public interface Registry<T> {
    /**
     * Register some object of arbitrary type to the registry.
     *
     * @param object The object to register.
     */
    T register(T object);

    /**
     * Unregister said object from this {@link Registry}.
     *
     * @param object The object to remove.
     */
    T unregister(T object);

    /**
     * Provides some mechanism to clear all registered entries from the underlying store.
     */
    void reset();

    /**
     * Returns the object associated with the identifier. A key of type {@link String} is used, therefore we should
     * protect against the case where an no item of type *T* mapped by {@param id} exists.
     * @param id The identifier used to query the underlying store.
     * @return The object of type *T* associated with {@param id}.
     */
    Optional<T> get(String id);
}
