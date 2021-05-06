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

import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.Attributes;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Defines Table Segment-specific Core Attributes.
 */
public class TableAttributes extends Attributes {
    /**
     * Defines an attribute that is used to store the first offset of a (Table) Segment that has not yet been indexed.
     */
    public static final AttributeId INDEX_OFFSET = AttributeId.uuid(CORE_ATTRIBUTE_ID_PREFIX, TABLE_ATTRIBUTES_START_OFFSET);

    /**
     * Defines an attribute that is used to store the number of indexed Table Entries in a (Table) Segment.
     */
    public static final AttributeId ENTRY_COUNT = AttributeId.uuid(CORE_ATTRIBUTE_ID_PREFIX, TABLE_ATTRIBUTES_START_OFFSET + 1);

    /**
     * Defines an attribute that is used to store the number of Table Buckets in a (Table) Segment.
     */
    public static final AttributeId BUCKET_COUNT = AttributeId.uuid(CORE_ATTRIBUTE_ID_PREFIX, TABLE_ATTRIBUTES_START_OFFSET + 2);

    /**
     * Defines an attribute that is used to store number of entries (active and overwritten) in a (Table) Segment.
     */
    public static final AttributeId TOTAL_ENTRY_COUNT = AttributeId.uuid(CORE_ATTRIBUTE_ID_PREFIX, TABLE_ATTRIBUTES_START_OFFSET + 3);

    /**
     * Defines an attribute that is used to store the offset of a (Table) Segment where compaction has last run at.
     */
    public static final AttributeId COMPACTION_OFFSET = AttributeId.uuid(CORE_ATTRIBUTE_ID_PREFIX, TABLE_ATTRIBUTES_START_OFFSET + 4);

    /**
     * Defines an attribute that is used to set the minimum utilization (as a percentage of {@link #ENTRY_COUNT} out of
     * {@link #TOTAL_ENTRY_COUNT}) of a Table Segment below which a Table Compaction is triggered.
     */
    public static final AttributeId MIN_UTILIZATION = AttributeId.uuid(CORE_ATTRIBUTE_ID_PREFIX, TABLE_ATTRIBUTES_START_OFFSET + 5);

    /**
     * [Retired April 2021. Do not reuse as obsolete values may linger around]
     * Whether the Table Segment is Sorted (Defunct functionality).
     */
    public static final AttributeId RETIRED_1 = AttributeId.uuid(CORE_ATTRIBUTE_ID_PREFIX, TABLE_ATTRIBUTES_START_OFFSET + 6);

    /**
     * Defines a Map that contains all Table Attributes along with their default values.
     */
    public static final Map<AttributeId, Long> DEFAULT_VALUES = Collections.unmodifiableMap(
            Arrays.stream(TableAttributes.class.getDeclaredFields())
                  .filter(f -> f.getType().equals(AttributeId.class))
                  .collect(Collectors.toMap(f -> {
                      try {
                          return (AttributeId) f.get(null);
                      } catch (IllegalAccessException ex) {
                          throw new RuntimeException(ex);
                      }
                  }, f -> 0L)));
}
