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
package io.pravega.client.state.impl;

import io.pravega.client.state.Revision;
import io.pravega.client.stream.Segment;
import com.google.common.base.Preconditions;

import java.io.Serializable;

import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@EqualsAndHashCode
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
@ToString
public class RevisionImpl implements Revision, Serializable {

    private static final long serialVersionUID = 1L;
    @Getter(value = AccessLevel.PACKAGE)
    private final Segment segment;
    @Getter(value = AccessLevel.PACKAGE)
    private final long offsetInSegment;
    @Getter(value = AccessLevel.PACKAGE)
    private final int eventAtOffset;

    @Override
    public int compareTo(Revision o) {
        Preconditions.checkArgument(segment.equals(o.asImpl().getSegment()));
        int result = Long.compare(offsetInSegment, o.asImpl().offsetInSegment);
        return result != 0 ? result : Integer.compare(eventAtOffset, o.asImpl().eventAtOffset); 
    }

    @Override
    public RevisionImpl asImpl() {
        return this;
    }

}
