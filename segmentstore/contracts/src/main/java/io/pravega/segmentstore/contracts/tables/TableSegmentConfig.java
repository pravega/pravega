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
package io.pravega.segmentstore.contracts.tables;

import lombok.Builder;
import lombok.Getter;

/**
 * Configuration for a Table Segment.
 */
@Builder
@Getter
public class TableSegmentConfig {
    public static final TableSegmentConfig NO_CONFIG = TableSegmentConfig.builder().build();
    @Builder.Default
    private final int keyLength = 0;
    @Builder.Default
    private final long rolloverSizeBytes = 0L;

    @Override
    public String toString() {
        return String.format("KeyLength = %s, RolloverSizeBytes = %s", this.keyLength, this.rolloverSizeBytes);
    }
}
