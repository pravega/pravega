/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

/**
 * StreamProperty is an abstraction wrapper over stream properties that can be updated via a workflow.
 * Namely @StreamConfiguration and @StreamTruncationRecord.
 * StreamProperty is a wrapper over actual property with an `updating` flag that indicates when a property is bring updated.
 */
@Data
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class StreamProperty<T> implements Serializable {
    private final T property;
    private final boolean updating;

    public static <T> StreamProperty<T> update(final T update) {
        Preconditions.checkNotNull(update);

        return new StreamProperty<>(update, true);
    }

    public static <T> StreamProperty<T> complete(final T complete) {
        Preconditions.checkNotNull(complete);

        return new StreamProperty<>(complete, false);
    }
}
