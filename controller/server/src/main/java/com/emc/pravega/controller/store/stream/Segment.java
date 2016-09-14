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

/**
 * In-memory representation of a stream segment.
 */
import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

@Data
@ToString(includeFieldNames = true)
public class Segment {

    enum Status {
        Active,
        Sealing,
        Sealed,
    }

    private final int number;
    private final long start;
    private final long end;
    private final double keyStart;
    private final double keyEnd;
    private final Status status;
    private final List<Integer> successors;
    private final List<Integer> predecessors;

    Segment(int number, long start, long end, double keyStart, double keyEnd) {
        this.number = number;
        this.start = start;
        this.end = end;
        this.keyStart = keyStart;
        this.keyEnd = keyEnd;
        this.status = Status.Active;
        successors = new ArrayList<>();
        predecessors = new ArrayList<>();
    }

    Segment(int number, long start, long end, double keyStart, double keyEnd, Status status, List<Integer> successors, List<Integer> predecessors) {
        Preconditions.checkNotNull(successors);
        Preconditions.checkNotNull(predecessors);
        this.number = number;
        this.start = start;
        this.end = end;
        this.keyStart = keyStart;
        this.keyEnd = keyEnd;
        this.status = status;
        this.successors = successors;
        this.predecessors = predecessors;
    }

    public boolean overlaps(Segment segment) {
        return segment.getKeyEnd() > keyStart && segment.getKeyStart() < keyEnd;
    }

    public boolean overlaps(double keyStart, double keyEnd) {
        return keyEnd > this.keyStart && keyStart < this.keyEnd;
    }
}
