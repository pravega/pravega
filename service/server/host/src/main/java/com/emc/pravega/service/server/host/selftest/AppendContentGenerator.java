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

import com.emc.pravega.common.util.BitConverter;
import com.google.common.base.Preconditions;

import java.util.Random;

/**
 * Generates arbitrary append contents, that can be deserialized and verified later.
 */
class AppendContentGenerator {
    //region Members

    private static final int HEADER_LENGTH = Long.BYTES;
    private static final int KEY_LENGTH = Integer.BYTES;
    private static final int LENGTH_LENGTH = Integer.BYTES;
    private static final int OVERHEAD_LENGTH = HEADER_LENGTH + KEY_LENGTH + LENGTH_LENGTH;
    private static final int PREFIX = (int) Math.pow(Math.E, 20);
    private final Random keyGenerator;
    private final int appendId;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the AppendContentGenerator class.
     *
     * @param appendId The Id to attach to all appends generated with this instance.
     */
    AppendContentGenerator(int appendId) {
        this.appendId = appendId;
        this.keyGenerator = new Random(appendId);
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
    public byte[] newAppend(int length) {
        Preconditions.checkArgument(length >= OVERHEAD_LENGTH, "length is insufficient to accommodate overhead.");
        int key = this.keyGenerator.nextInt();
        byte[] result = new byte[length];

        // Header: PREFIX + appendId
        int offset = 0;
        offset += BitConverter.writeInt(result, offset, PREFIX);
        offset += BitConverter.writeInt(result, offset, this.appendId);

        // Key
        offset += BitConverter.writeInt(result, offset, key);

        // Length
        int contentLength = length - OVERHEAD_LENGTH;
        offset += BitConverter.writeInt(result, offset, contentLength);

        // Content
        writeContent(result, offset, contentLength, key);
        return result;
    }

    private static void writeContent(byte[] result, int offset, int length, int key) {
        Random contentGenerator = new Random(key);
        while (offset < length) {
            int value = contentGenerator.nextInt();

            for (int var5 = Math.min(length - offset, 4); var5-- > 0; value >>= 8) {
                result[offset++] = (byte) value;
            }
        }
    }

    //endregion
}
