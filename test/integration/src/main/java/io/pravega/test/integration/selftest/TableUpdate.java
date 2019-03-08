/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.selftest;

import io.pravega.common.io.EnhancedByteArrayOutputStream;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.ByteArraySegment;
import java.util.Random;
import java.util.UUID;
import lombok.Getter;
import lombok.val;

/**
 * An update to a Table.
 */
@Getter
class TableUpdate implements ProducerUpdate {
    private static final byte[] NEGATIVE = "minus".getBytes();
    private static final byte[][] NUMBERS = new byte[][]{
            "zero".getBytes(),
            "one".getBytes(),
            "two".getBytes(),
            "three".getBytes(),
            "four".getBytes(),
            "five".getBytes(),
            "six".getBytes(),
            "seven".getBytes(),
            "eight".getBytes(),
            "nine".getBytes()
    };
    private static final Random RANDOM = new Random();

    private final UUID keyId;
    private final ArrayView key;
    private final ArrayView value;

    /**
     * If non-null, this is a conditional update/removal, and this represents the condition.
     */
    private final Long version;

    /**
     * If true, this should result in a Removal (as opposed from an update).
     */
    private final boolean removal;

    //region Constructor

    private TableUpdate(UUID keyId, Long version, ArrayView key, ArrayView value, boolean isRemoval) {
        this.keyId = keyId;
        this.version = version;
        this.key = key;
        this.value = value;
        this.removal = isRemoval;
    }

    static TableUpdate update(UUID keyId, int valueLength, Long version) {
        return new TableUpdate(keyId, version, generateKey(keyId), generateValue(valueLength), false);
    }

    static TableUpdate removal(UUID keyId, Long version) {
        return new TableUpdate(keyId, version, generateKey(keyId), null, true);
    }

    //endregion

    //region Properties

    @Override
    public String toString() {
        return String.format("%s KeyId:%s, Version:%s",
                isRemoval() ? "Remove" : "Update", this.keyId, this.version == null ? "(null)" : this.version.toString());
    }

    //endregion

    private static ArrayView generateKey(UUID keyId) {
        // We "serialize" the KeyId using English words for each digit.
        val r = new EnhancedByteArrayOutputStream();
        add(keyId.getMostSignificantBits(), r);
        add(keyId.getLeastSignificantBits(), r);
        return r.getData();
    }

    private static ArrayView generateValue(int length) {
        val r = new byte[length];
        RANDOM.nextBytes(r);
        return new ByteArraySegment(r);
    }

    private static void add(long number, EnhancedByteArrayOutputStream s) {
        if (number < 0) {
            number = -number;
            s.write(NEGATIVE);
        }

        do {
            s.write(NUMBERS[(int) (number % 10)]);
            number /= 10;
        } while (number != 0);
    }
}
