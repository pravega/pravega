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
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Arrays;
import lombok.NonNull;

/**
 * Array Wrapper that provides a {@link Object#hashCode()} and {@link Object#equals(Object)} method.
 * Suitable for using as {@link java.util.HashMap} key.
 */
public class HashedArray implements ArrayView {
    private static final HashHelper HASH = HashHelper.seededWith(HashedArray.class.getName());
    protected final byte[] array;
    private final int hashCode;

    /**
     * Creates a new instance of the HashedArray class.
     * @param array An array to wrap.
     */
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

    //region ArrayView Implementation

    @Override
    public byte get(int index) {
        return this.array[index];
    }

    @Override
    public int getLength() {
        return this.array.length;
    }

    @Override
    public byte[] array() {
        return this.array;
    }

    @Override
    public int arrayOffset() {
        return 0;
    }

    @Override
    public InputStream getReader() {
        return new ByteArrayInputStream(this.array);
    }

    @Override
    public InputStream getReader(int offset, int length) {
        return new ByteArrayInputStream(this.array, offset, length);
    }

    @Override
    public void copyTo(byte[] target, int targetOffset, int length) {
        System.arraycopy(this.array, 0, target, targetOffset, length);
    }

    @Override
    public byte[] getCopy() {
        return Arrays.copyOf(this.array, this.array.length);
    }

    //endregion
}