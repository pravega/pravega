/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.tables.hashing;

import io.pravega.common.hash.HashHelper;
import lombok.Getter;
import lombok.NonNull;

/**
 * Array Wrapper that provides a HashCode and Equals method. Suitable for using as HashMap key.
 * TODO: consider integrating into ArrayView.
 */
public class HashedArray {
    private static final HashHelper HASH = HashHelper.seededWith(HashedArray.class.getName());
    @Getter
    protected final byte[] array;
    private final int hashCode;

    public HashedArray(@NonNull byte[] array) {
        this.array = array;
        this.hashCode = HASH.hash(array, 0, array.length);
    }

    @Override
    public int hashCode() {
        return this.hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof HashedArray) {
            HashedArray ha = (HashedArray) obj;
            return this.hashCode == ha.hashCode && arrayEquals(this.array, ha.array);
        }

        return false;
    }

    @Override
    public String toString() {
        return String.format("Length=%d, Hash=%d", this.array.length, this.hashCode);
    }

    private boolean arrayEquals(byte[] a1, byte[] a2) {
        if (a1.length != a2.length) {
            return false;
        }

        for (int i = 0; i < a1.length; i++) {
            if (a1[i] != a2[i]) {
                return false;
            }
        }

        return true;
    }
}