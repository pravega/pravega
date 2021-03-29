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
package io.pravega.client.tables;

import com.google.common.annotations.Beta;
import java.io.Serializable;
import lombok.Builder;
import lombok.Data;

/**
 * The configuration of a Key-Value Table.
 */
@Beta
@Data
@Builder
public class KeyValueTableConfiguration implements Serializable {
    private static final long serialVersionUID = 1L;
    /**
     * The number of Partitions for a Key-Value Table. This value cannot be adjusted after the Key-Value Table has been
     * created.
     *
     * @param partitionCount The number of Partitions for a Key-Value Table.
     * @return The number of Partitions for a Key-Value Table.
     */
    private final int partitionCount;
}
