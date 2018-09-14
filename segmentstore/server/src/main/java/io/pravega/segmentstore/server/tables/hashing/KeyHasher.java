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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.ByteArraySegment;
import java.util.function.Function;
import lombok.NonNull;

/**
 * Defines a Hasher for a Table Key.
 */
public abstract class KeyHasher {
    protected final HashConfig config;

    private KeyHasher(@NonNull HashConfig config) {
        this.config = config;
        Preconditions.checkArgument(getHashLengthBytes() >= config.getMinHashLengthBytes(),
                "KeyHasher '%s' produces %s-byte hashes, but config requires at least %s.", this,
                getHashLengthBytes(), config.getMinHashLengthBytes());
    }

    /**
     * Generates a new {@link KeyHash} for the given Key.
     *
     * @param key The Key to hash.
     * @return A new {@link KeyHash}.
     */
    public KeyHash hash(@NonNull byte[] key){
        return hash(new ByteArraySegment(key));
    }

    /**
     * Generates a new {@link KeyHash} for the given Key.
     *
     * @param key The Key to hash.
     * @return A new {@link KeyHash}.
     */
    public abstract KeyHash hash(@NonNull ArrayView key);

    /**
     * Gets a value indicating the number of Hash Parts this KeyHasher generates (based on the given {@link HashConfig}).
     */
    public int getHashPartCount() {
        return this.config.getHashCount();
    }

    /**
     * When overridden in a derived class, this method returns the length of the generated Hash, in bytes.
     */
    protected abstract int getHashLengthBytes();

    /**
     * Creates a new instance of the KeyHasher class that generates 64-byte SHA-512 hashes.
     *
     * @param config The {@link HashConfig} to use.
     * @return A new instance of the KeyHasher class.
     */
    public static KeyHasher sha512(HashConfig config) {
        return new Sha512Hasher(config);
    }

    /**
     * Creates a new instance of the KeyHasher class that generates custom hashes, based on the given Function.
     *
     * @param hashFunction A Function that, given an {@link ArrayView}, produces a byte array representing its hash. The
     *                     length of this hash must match the value from {@link HashConfig#getMinHashLengthBytes()}.
     * @param config       The {@link HashConfig} to use.
     * @return A new instance of the KeyHasher class.
     */
    @VisibleForTesting
    public static KeyHasher custom(Function<ArrayView, byte[]> hashFunction, HashConfig config) {
        return new CustomHasher(hashFunction, config);
    }

    //region Sha512Hasher

    private static class Sha512Hasher extends KeyHasher {
        private final HashFunction hash = Hashing.sha512();

        private Sha512Hasher(HashConfig config) {
            super(config);
        }

        @Override
        public KeyHash hash(@NonNull ArrayView key) {
            return new KeyHash(this.hash.hashBytes(key.array(), key.arrayOffset(), key.getLength()).asBytes(), this.config);
        }

        @Override
        protected int getHashLengthBytes() {
            return 64; // 512 bits.
        }
    }

    //endregion

    //region CustomHasher

    private static class CustomHasher extends KeyHasher {
        private final Function<ArrayView, byte[]> hashFunction;

        private CustomHasher(@NonNull Function<ArrayView, byte[]> hashFunction, HashConfig config) {
            super(config);
            this.hashFunction = hashFunction;
        }

        @Override
        public KeyHash hash(@NonNull ArrayView key) {
            byte[] rawHash = this.hashFunction.apply(key);
            Preconditions.checkState(rawHash.length == getHashLengthBytes(), "Resulting KeyHash has incorrect length.");
            return new KeyHash(rawHash, this.config);
        }

        @Override
        protected int getHashLengthBytes() {
            return this.config.getMinHashLengthBytes();
        }
    }

    //endregion
}