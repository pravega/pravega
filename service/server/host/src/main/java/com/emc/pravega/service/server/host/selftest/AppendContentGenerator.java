/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.server.host.selftest;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.util.ArrayView;
import com.emc.pravega.common.util.BitConverter;
import com.google.common.base.Preconditions;

import java.util.Random;

/**
 * Generates arbitrary append contents, that can be deserialized and verified later.
 */
class AppendContentGenerator {
    //region Members

    private static final int PREFIX_LENGTH = Integer.BYTES;
    private static final int OWNER_ID_LENGTH = Integer.BYTES;
    private static final int KEY_LENGTH = Integer.BYTES;
    private static final int LENGTH_LENGTH = Integer.BYTES;
    static final int HEADER_LENGTH = PREFIX_LENGTH + OWNER_ID_LENGTH + KEY_LENGTH + LENGTH_LENGTH;
    private static final int PREFIX = (int) Math.pow(Math.E, 20);
    private final Random keyGenerator;
    private final int ownerId;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the AppendContentGenerator class.
     *
     * @param ownerId The Id to attach to all appends generated with this instance.
     */
    AppendContentGenerator(int ownerId) {
        this.ownerId = ownerId;
        this.keyGenerator = new Random(ownerId);
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

        // Header: PREFIX + ownerId + Key + Length
        int offset = 0;
        offset += BitConverter.writeInt(result, offset, PREFIX);
        offset += BitConverter.writeInt(result, offset, this.ownerId);
        offset += BitConverter.writeInt(result, offset, key);
        int contentLength = length - HEADER_LENGTH;
        offset += BitConverter.writeInt(result, offset, contentLength);

        // Content
        writeContent(result, offset, contentLength, key);
        return result;
    }

    private static void writeContent(byte[] result, int offset, int length, int key) {
        Random contentGenerator = new Random(key);
        while (offset < length) {
            int value = contentGenerator.nextInt();

            for (int counter = Math.min(length - offset, 4); counter-- > 0; value >>= 8) {
                result[offset++] = (byte) value;
            }
        }
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
        int ownerId = BitConverter.readInt(view, offset);
        offset += OWNER_ID_LENGTH;

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

        return validateContent(view, offset, length, key);
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

    //region ValidationResult

    /**
     * Represents the result of a validation process.
     */
    static class ValidationResult {
        //region Members

        private boolean moreDataNeeded;
        private int length;
        private String failureMessage;

        //endregion

        //region Constructor

        private ValidationResult() {
            this.moreDataNeeded = false;
            this.length = HEADER_LENGTH;
            this.failureMessage = null;
        }

        /**
         * Creates a new ValidationResult for a failed verification.
         */
        private static ValidationResult failed(String message) {
            Exceptions.checkNotNullOrEmpty(message, "message");
            ValidationResult result = new ValidationResult();
            result.failureMessage = message;
            return result;
        }

        /**
         * Creates a new ValidationResult for an inconclusive verification, when more data is needed to determine correctness.
         */
        private static ValidationResult moreDataNeeded() {
            ValidationResult result = new ValidationResult();
            result.moreDataNeeded = true;
            return result;
        }

        /**
         * Creates a new ValidationResult for a successful test.
         */
        private static ValidationResult success(int length) {
            ValidationResult result = new ValidationResult();
            result.length = HEADER_LENGTH + length;
            return result;
        }

        //endregion

        /**
         * Gets a value indicating whether more data is needed in order to make a proper determination.
         * If this is true, it does not mean that the test failed.
         */
        boolean isMoreDataNeeded() {
            return this.moreDataNeeded;
        }

        /**
         * Gets a value indicating whether the verification failed.
         */
        boolean isFailed() {
            return this.failureMessage != null;
        }

        /**
         * Gets a value indicating whether the verification succeeded.
         */
        boolean isSuccess() {
            return !isFailed() && !isMoreDataNeeded();
        }

        /**
         * Gets a value indicating the failure message. This is undefined if isFailed() == false.
         */
        String getFailureMessage() {
            return this.failureMessage;
        }

        /**
         * Gets a value indicating the length of the validated append. This value is undefined if isSuccess() == false.
         */
        int getLength() {
            Preconditions.checkState(isSuccess(), "Can only request length if a successful validation result.");
            return this.length;
        }

        @Override
        public String toString() {
            if (isFailed()) {
                return String.format("Failed (%s)", this.failureMessage);
            } else if (isMoreDataNeeded()) {
                return "More data needed";
            } else {
                return String.format("Success (Length = %d)", this.length);
            }
        }
    }

    //endregion
}
