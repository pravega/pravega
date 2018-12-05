/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util;

import io.pravega.common.hash.HashHelper;

/**
 * Byte Array Wrapper that provides a {@link Object#hashCode()} and {@link Object#equals(Object)} method.
 * Suitable for using as {@link java.util.HashMap} key.
 */
public class HashedArray extends ByteArraySegment {
    private static final HashHelper HASH = HashHelper.seededWith(HashedArray.class.getName());
    private final int hashCode;

    /**
     * Creates a new instance of the HashedArray class.
     * @param array A byte array to wrap.
     */
    public HashedArray(byte[] array) {
        super(array, 0, array.length);
        this.hashCode = HASH.hash(array, 0, array.length);
    }

    /**
     * Creates a new instance of the HashedArray class.
     *
     * @param array An {@link ArrayView} to wrap.
     */
    public HashedArray(ArrayView array) {
        super(array.array(), array.arrayOffset(), array.getLength());
        this.hashCode = HASH.hash(array.array(), array.arrayOffset(), array.getLength());
    }

    @Override
    public int hashCode() {
        return this.hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof HashedArray) {
            HashedArray ha = (HashedArray) obj;
            return this.hashCode == ha.hashCode && arrayEquals(ha);
        }

        return false;
    }

    @Override
    public String toString() {
        return String.format("Length=%d, Hash=%d", getLength(), this.hashCode);
    }

    private boolean arrayEquals(HashedArray other) {
        int len = getLength();
        if (len != other.getLength()) {
            return false;
        }

        byte[] a1 = this.array();
        int o1 = this.arrayOffset();
        byte[] a2 = other.array();
        int o2 = other.arrayOffset();
        for (int i = 0; i < len; i++) {
            if (a1[o1 + i] != a2[o2 + i]) {
                return false;
            }
        }

        return true;
    }
}