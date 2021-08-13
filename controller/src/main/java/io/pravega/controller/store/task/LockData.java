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
package io.pravega.controller.store.task;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.SerializationUtils;

import java.io.Serializable;

/**
 * Lock row
 */
@Data
@EqualsAndHashCode
class LockData implements Serializable {
    private final String hostId;
    private final String tag;
    private final byte[] taskData;

    public byte[] serialize() {
        return SerializationUtils.serialize(this);
    }

    public boolean isOwnedBy(final String owner, final String ownerTag) {
        return hostId != null
                && hostId.equals(owner)
                && tag != null
                && tag.equals(ownerTag);
    }

    public static LockData deserialize(final byte[] bytes) {
        return (LockData) SerializationUtils.deserialize(bytes);
    }
}
