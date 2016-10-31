package com.emc.pravega.common.concurrent;

import lombok.RequiredArgsConstructor;
import lombok.Synchronized;

@RequiredArgsConstructor
public final class NewestRefrence<T extends Comparable<T>> {
    private T value;

    @Synchronized
    public T get() {
        return value;
    }

    @Synchronized
    public void update(T newValue) {
        if (newValue != null && (value == null || value.compareTo(newValue) < 0)) {
            value = newValue;
        }
    }
}
