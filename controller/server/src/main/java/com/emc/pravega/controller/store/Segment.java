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
package com.emc.pravega.controller.store;

/**
 * In-memory representation of a stream segment.
 */
import lombok.Data;
import lombok.ToString;

import java.util.List;

@Data
@ToString(includeFieldNames=true)
public class Segment {

    enum Status {
        Active,
        Sealing,
        Sealed,
    }

    private int number;
    private long start;
    private long end;
    private double keyStart;
    private double keyEnd;
    private Status status;
    private List<Integer> successors;
    private List<Integer> predecessors;

    public Segment(int number, long start, long end, double keyStart, double keyEnd) {
        this.number = number;
        this.start = start;
        this.end = end;
        this.keyStart = keyStart;
        this.keyEnd = keyEnd;
    }

    public Segment(int number, long start, long end, double keyStart, double keyEnd, List<Integer> successors, List<Integer> predecessors) {
        this.number = number;
        this.start = start;
        this.end = end;
        this.keyStart = keyStart;
        this.keyEnd = keyEnd;
        this.successors = successors;
        this.predecessors = predecessors;
    }
}
