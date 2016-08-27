/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.controller.store.stream;

import lombok.Data;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * List of segments from where a consumer can start reading, or a producer can start producing to.
 * For each segment, SegmentFutures also lists all the segments that can be read from (produced to) after
 * that segment is completely read (sealed).
 */
@Data
public class SegmentFutures {
    // current segments to read from or write to
    private final List<Integer> current;

    // future segments to read from or write to when curent segment is completely read (consumer) or sealed (producer)
    private final Map<Integer, Integer> futures;

    SegmentFutures(List<Integer> current, Map<Integer, Integer> futures) {
        this.current = Collections.unmodifiableList(current);
        this.futures = Collections.unmodifiableMap(futures);
    }
}
