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
package io.pravega.client.segment.impl;

import io.pravega.common.hash.HashHelper;
import io.pravega.shared.protocol.netty.WireCommands;
import java.util.UUID;
import lombok.Getter;

/**
 * Attributes that can be set by the client on a segment.
 */
public enum SegmentAttribute {

    RevisionStreamClientMark;
    
    public static final long NULL_VALUE = WireCommands.NULL_ATTRIBUTE_VALUE;
    
    @Getter
    private final UUID value;
    
    SegmentAttribute() {
        HashHelper hash = HashHelper.seededWith("SegmentAttribute");
        value = hash.toUUID(this.name());
    }
}
