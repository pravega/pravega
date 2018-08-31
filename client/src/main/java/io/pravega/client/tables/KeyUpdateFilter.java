/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.tables;

import com.google.common.base.Preconditions;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * Represents a Key filter for a {@link KeyUpdateListener}. Only Keys matching this filter will have callbacks invoked.
 *
 * @param <KeyT>
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class KeyUpdateFilter<KeyT> {
    private final Set<KeyT> filterKeys;

    /**
     * Creates a new instance of the {@link KeyUpdateFilter} class that will match ALL Keys in the Table.
     *
     * @param <KeyT> Type of the Key.
     * @return A new instance of the {@link KeyUpdateFilter}.
     */
    static <KeyT> KeyUpdateFilter<KeyT> allKeys() {
        return new KeyUpdateFilter<>(Collections.emptySet());
    }

    /**
     * Creates a new instance of the {@link KeyUpdateFilter} class that will match some Keys in the Table.
     *
     * @param keys   A non-empty Collection of keys to register updates for.
     * @param <KeyT> Type of the Key.
     * @return A new instance of the {@link KeyUpdateFilter}.
     */
    static <KeyT> KeyUpdateFilter<KeyT> forKeys(@NonNull Collection<KeyT> keys) {
        Preconditions.checkArgument(!keys.isEmpty(), "keys must not be empty.");
        return new KeyUpdateFilter<>(Collections.unmodifiableSet(new HashSet<>(keys)));
    }
}
