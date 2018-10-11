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

import com.google.common.collect.Iterators;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.HashedArray;
import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.NonNull;
import org.apache.commons.lang3.tuple.Pair;

/**
 * A Hash for a Table Key.
 */
public class KeyHash extends HashedArray implements Iterable<ArrayView> {
    private final ByteArraySegment[] parts;

    /**
     * Creates a new instance of the KeyHash class.
     *
     * @param hash   The raw hash.
     * @param config Hash Configuration.
     */
    KeyHash(byte[] hash, @NonNull HashConfig config) {
        super(hash);
        this.parts = new ByteArraySegment[config.getHashCount()];
        for (int i = 0; i < this.parts.length; i++) {
            Pair<Integer, Integer> offset = config.getOffsets(i);
            this.parts[i] = new ByteArraySegment(hash, offset.getLeft(), offset.getRight() - offset.getLeft());
        }
    }

    /**
     * Gets the hash part with given index (based on the HashConfig passed into the constructor of this class).
     *
     * @param index Hash index.
     * @return An ArrayView representing the hash with given index.
     */
    public ByteArraySegment getPart(int index) {
        return this.parts[index];
    }

    /**
     * Gets a value representing the number of hash parts.
     *
     * @return The hash part count.
     */
    public int hashCount() {
        return this.parts.length;
    }

    @Override
    @Nonnull
    public Iterator<ArrayView> iterator() {
        return Iterators.forArray(this.parts);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    private String getSignature() {
        return Arrays.stream(this.parts)
                     .map(p -> p.get(p.getLength() - 1))
                     .map(Object::toString)
                     .collect(Collectors.joining(":"));
    }

    @Override
    public String toString() {
        return String.format("%s, Sig=%s", super.toString(), getSignature());
    }
}
