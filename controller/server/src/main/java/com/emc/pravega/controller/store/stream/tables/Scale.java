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
package com.emc.pravega.controller.store.stream.tables;

import lombok.Data;

import java.util.AbstractMap;
import java.util.List;

@Data
/**
 * Task subclass to define scaling operations
 * This is serialized and stored in the persistent store
 * and used to resume partially completed scale operation
 */ public class Scale {
    private final List<Integer> sealedSegments;
    private final List<AbstractMap.SimpleEntry<Double, Double>> newRanges;
    private final long scaleTimestamp;
}
