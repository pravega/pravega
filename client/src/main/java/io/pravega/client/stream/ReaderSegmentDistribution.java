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
package io.pravega.client.stream;

import lombok.Builder;
import lombok.Getter;

import java.util.Map;

/**
 * Describes current distribution of number of segments assigned to each reader in the reader group.
 * It contains a count of unassigned segments and a map of readerId to segment count.
 */
@Builder
public class ReaderSegmentDistribution {
    @Getter
    private final Map<String, Integer> readerSegmentDistribution;
    @Getter
    private final int unassignedSegments;
}
