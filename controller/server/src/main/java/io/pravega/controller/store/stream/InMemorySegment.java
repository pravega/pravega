/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
package io.pravega.controller.store.stream;

/**
 * In-memory representation of a stream segment.
 */
import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

@Data
@ToString(includeFieldNames = true)
@EqualsAndHashCode(callSuper = true)
public class InMemorySegment extends Segment {

    enum Status {
        Active,
        Sealing,
        Sealed,
    }

    private final long end;
    private final Status status;
    private final List<Integer> successors;
    private final List<Integer> predecessors;

    InMemorySegment(final int number, final long start, final long end, final double keyStart, final double keyEnd) {
        super(number, start, keyStart, keyEnd);
        this.end = end;
        this.status = Status.Active;
        successors = new ArrayList<>();
        predecessors = new ArrayList<>();
    }

    InMemorySegment(final int number,
                    final long start,
                    final long end,
                    final double keyStart,
                    final double keyEnd,
                    final Status status,
                    final List<Integer> successors,
                    final List<Integer> predecessors) {
        super(number, start, keyStart, keyEnd);
        Preconditions.checkNotNull(successors);
        Preconditions.checkNotNull(predecessors);
        this.end = end;
        this.status = status;
        this.successors = successors;
        this.predecessors = predecessors;
    }
}
