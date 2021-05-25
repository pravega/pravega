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
package io.pravega.client.admin;

import com.google.common.annotations.Beta;
import io.pravega.shared.NameUtils;
import lombok.Data;

/**
 * Information about a Key-Value Table.
 */
@Data
@Beta
public class KeyValueTableInfo {
    /**
     * Scope name of the Key-Value Table.
     *
     * @param scope Scope name of the Key-Value Table.
     * @return Scope name of the Key-Value Table.
     */
    private final String scope;

    /**
     * Key-Value Table name.
     *
     * @param keyValueTableName Key-Value Table name.
     * @return Key-Value Table name.
     */
    private final String keyValueTableName;

    /**
     * Gets a Fully Qualified Key-Value Table Name.
     * @return The scoped Key-Value Table Name.
     */
    public String getScopedName() {
        return NameUtils.getScopedKeyValueTableName(this.scope, this.keyValueTableName);
    }
}
