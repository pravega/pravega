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
package io.pravega.segmentstore.storage.chunklayer;

import lombok.Builder;
import lombok.Data;

/**
 * Information about the storage usage stats.
 */
@Data
@Builder
public class StorageCapacityStats {
    /**
     * Total space in bytes.
     */
    private long totalSpace;

    /**
     *  Used space in bytes.
     */
    private long usedSpace;

    /**
     * Calculates used space in percentage.
     * @return returns calculated value.
     */
    public double getUsedPercentage() {
        return (100.0 * usedSpace) / totalSpace;
    }
}
