/**
 * Copyright Pravega Authors.
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
package io.pravega.test.integration.selftest;

import io.pravega.common.util.BufferView;
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
    private static final Random RANDOM = new Random();

    private final UUID keyId;
    private final BufferView key;
    private final BufferView value;

    /**
     * If non-null, this is a conditional update/removal, and this represents the condition.
     */
    private final Long version;

    /**
     * If true, this should result in a Removal (as opposed from an update).
     */
    private final boolean removal;

    //region Constructor

    private TableUpdate(UUID keyId, Long version, BufferView key, BufferView value, boolean isRemoval) {
        this.keyId = keyId;
        this.version = version;
        this.key = key;
        this.value = value;
        this.removal = isRemoval;
    }

    static TableUpdate update(UUID keyId, int keyLength, int valueLength, Long version) {
        return new TableUpdate(keyId, version, generateKey(keyId, keyLength), generateValue(valueLength), false);
    }

    static TableUpdate removal(UUID keyId, int keyLength, Long version) {
        return new TableUpdate(keyId, version, generateKey(keyId, keyLength), null, true);
    }

    //endregion

    //region Properties

    @Override
    public void release() {
        // Nothing to do.
    }

    @Override
    public String toString() {
        return String.format("%s KeyId:%s, Version:%s",
                isRemoval() ? "Remove" : "Update", this.keyId, this.version == null ? "(null)" : this.version.toString());
    }

    //endregion

    static BufferView generateKey(UUID keyId, int keyLength) {
        assert keyLength >= 8 : "keyLength must be at least 8 bytes";

        // We "serialize" the KeyId using English words for each digit.
        val result = new ByteArraySegment(new byte[keyLength]);
        int count = keyLength >> 4;
        int offset = 0;
        for (int i = 0; i < count; i++) {
            result.setLong(offset, keyId.getMostSignificantBits());
            result.setLong(offset, keyId.getLeastSignificantBits());
            offset += 16;
        }

        if (keyLength - offset >= 8) {
            result.setLong(offset, keyId.getMostSignificantBits());
        }

        return result;
    }

    private static BufferView generateValue(int length) {
        val r = new byte[length];
        RANDOM.nextBytes(r);
        return new ByteArraySegment(r);
    }
}
