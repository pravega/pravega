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
package io.pravega.service.contracts;

import java.util.UUID;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

/**
 * Represents an update to a value of an Attribute.
 */
@AllArgsConstructor
@Getter
@Setter
@EqualsAndHashCode(of = {"attributeId", "value"})
@NotThreadSafe
public class AttributeUpdate {
    private final UUID attributeId;
    private final AttributeUpdateType updateType;
    private long value;

    @Override
    public String toString() {
        return String.format("AttributeId = %s, Value = %s, UpdateType = %s", this.attributeId, this.value, this.updateType);
    }
}
