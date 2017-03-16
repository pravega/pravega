/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.common.util;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Created by andrei on 3/15/17.
 */
@RequiredArgsConstructor
public class Property<T> {
    @Getter
    private final String name;
    @Getter
    private final T defaultValue;

    Property(String name) {
        this(name, null);
    }

    boolean hasDefaultValue() {
        return this.defaultValue != null;
    }
}
