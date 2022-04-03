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
package io.pravega.client.stream.impl;

import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.EventPointer;
import lombok.AllArgsConstructor;

/**
 * Implementation of the EventPointerExternal interface. This Implementation
 * of methods uses the Implementation of EventPointerInternal methods.
 */
@AllArgsConstructor
public class EventPointerExternalImpl implements EventPointerExternal {
    private  final EventPointer eventPointer;

    @Override
    public Segment getSegment() {
        return eventPointer.asImpl().getSegment();
    }

    @Override
    public long getEventStartOffset() {
        return  eventPointer.asImpl().getEventStartOffset();
    }

    @Override
    public  int getEventLength() {
        return eventPointer.asImpl().getEventLength();
    }

}
