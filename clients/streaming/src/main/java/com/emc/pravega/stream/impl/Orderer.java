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
package com.emc.pravega.stream.impl;

import java.util.Collection;

/**
 * Used to select which event should go next when consuming from multiple segments.
 *
 * @param <Type> The type of events that are in the stream
 */
public interface Orderer<Type> {

    /**
     * Given a list of segment this reader owns, (which contain their positions) returns the one that
     * should be read from next. This is done in a consistent way. IE: Calling this method with the
     * same readers at the same positions, should yield the same result. (The passed collection is
     * not modified)
     *
     * @param segments The logs to get the next reader for.
     */
    SegmentReader<Type> nextSegment(Collection<SegmentReader<Type>> segments);
}
