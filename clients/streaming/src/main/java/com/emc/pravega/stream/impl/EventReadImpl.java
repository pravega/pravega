/**
 *  Copyright (c) 2017 Dell Inc. or its subsidiaries. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.stream.EventRead;
import com.emc.pravega.stream.Position;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Sequence;

import lombok.Data;

@Data
public class EventReadImpl<T> implements EventRead<T> {
    private final Sequence eventSequence;
    private final T event;
    private final Position position;
    private final Segment segment;
    private final Long offsetInSegment;
    private final boolean routingRebalance;
}
