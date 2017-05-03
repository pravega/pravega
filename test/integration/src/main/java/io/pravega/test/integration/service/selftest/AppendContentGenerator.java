/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.test.integration.service.selftest;

import io.pravega.common.util.ArrayView;
import io.pravega.common.util.BitConverter;
import com.google.common.base.Preconditions;

import java.time.Duration;
import java.util.Random;

/**
 * Generates arbitrary append contents, that can be deserialized and verified later.
 */
class AppendContentGenerator {
    //region Members

    private static final int PREFIX_LENGTH = Integer.BYTES;
    private static final int OWNER_ID_LENGTH = Integer.BYTES;
    private static final int START_TIME_LENGTH = Long.BYTES;
    private static final int KEY_LENGTH = Integer.BYTES;
    private static final int LENGTH_LENGTH = Integer.BYTES;
    static final int HEADER_LENGTH = PREFIX_LENGTH + OWNER_ID_LENGTH + START_TIME_LENGTH + KEY_LENGTH + LENGTH_LENGTH;
    private static final int PREFIX = (int) Math.pow(Math.E, 20);

    private final Random keyGenerator;
    private final int ownerId;
    private final boolean recordStartTime;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the AppendContentGenerator class.
     *
     * @param ownerId         The Id to attach to all appends generated with this instance.
     * @param recordStartTime Whether to record start times in the appends.
     */
    AppendContentGenerator(int ownerId, boolean recordStartTime) {
        this.ownerId = ownerId;
        this.keyGenerator = new Random(ownerId);
        this.recordStartTime = recordStartTime;
    }

    //endregion

    //region New Append

    /**
     * Generates a byte array containing data for an append.
     * Format: [Header][Key][Length][Contents]
     * * [Header]: is a sequence of bytes identifying the start of an append
     * * [Key]: a randomly generated sequence of bytes
     * * [Length]: length of [Contents]
     * * [Contents]: a deterministic result of [Key] & [Length].
     *
     * @param length The total length of the append (including overhead).
     * @return The generated array.
     */
    byte[] newAppend(int length) {
        Preconditions.checkArgument(length >= HEADER_LENGTH, "length is insufficient to accommodate header.");
        int key = this.keyGenerator.nextInt();
        byte[] result = new byte[length];

        // Header: PREFIX + ownerId + start time + Key + Length
        int offset = 0;
        offset += BitConverter.writeInt(result, offset, PREFIX);
        offset += BitConverter.writeInt(result, offset, this.ownerId);
        offset += BitConverter.writeLong(result, offset, this.recordStartTime ? getCurrentTimeNanos() : Long.MIN_VALUE);
        offset += BitConverter.writeInt(result, offset, key);
        int contentLength = length - HEADER_LENGTH;
        offset += BitConverter.writeInt(result, offset, contentLength);
        assert offset == HEADER_LENGTH : "Append header has a different length than expected";

        // Content
        writeContent(result, offset, contentLength, key);
        return result;
    }

    private void writeContent(byte[] result, int offset, int length, int key) {
        Random contentGenerator = new Random(key);
        while (offset < length) {
            int value = contentGenerator.nextInt();

            for (int counter = Math.min(length - offset, 4); counter-- > 0; value >>= 8) {
                result[offset++] = (byte) value;
            }
        }
    }

    private static long getCurrentTimeNanos() {
        return System.nanoTime();
    }

    //endregion

    //region Validation

    /**
     * Validates that the given ArrayView contains a valid Append, starting at the given offset.
     *
     * @param view   The view to inspect.
     * @param offset The offset to start inspecting at.
     * @return A ValidationResult representing the validation.
     */
    static ValidationResult validate(ArrayView view, int offset) {
        // Extract prefix and validate.
        int prefix = BitConverter.readInt(view, offset);
        offset += PREFIX_LENGTH;
        if (prefix != PREFIX) {
            return ValidationResult.failed("Prefix mismatch.");
        }

        // Extract ownerId.
        @SuppressWarnings("unused")
        int ownerId = BitConverter.readInt(view, offset);
        offset += OWNER_ID_LENGTH;

        // Extract start time.
        long startTimeNanos = BitConverter.readLong(view, offset);
        offset += START_TIME_LENGTH;

        // Extract key.
        int key = BitConverter.readInt(view, offset);
        offset += KEY_LENGTH;

        // Extract length.
        int length = BitConverter.readInt(view, offset);
        offset += LENGTH_LENGTH;
        if (length < 0) {
            return ValidationResult.failed("Append length cannot be negative.");
        }

        // Extract & validate data.
        if (offset + length > view.getLength()) {
            return ValidationResult.moreDataNeeded();
        }

        ValidationResult result = validateContent(view, offset, length, key);
        if (result.isSuccess() && startTimeNanos >= 0) {
            // Set the elapsed time, but only for successful operations that did encode the start time.
            result.setElapsed(Duration.ofNanos(getCurrentTimeNanos() - startTimeNanos));
        }

        return result;
    }

    private static ValidationResult validateContent(ArrayView view, int offset, int length, int key) {
        Random contentGenerator = new Random(key);
        while (offset < length) {
            int value = contentGenerator.nextInt();

            for (int counter = Math.min(length - offset, 4); counter-- > 0; value >>= 8) {
                if (view.get(offset) != (byte) value) {
                    return ValidationResult.failed("Append Content differ.");
                }

                offset++;
            }
        }

        return ValidationResult.success(length);
    }

    //endregion
}
