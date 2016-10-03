/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.stream;

import com.google.common.base.Preconditions;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;

/**
 * An identifier for a segment of a stream.
 */
@Data
@EqualsAndHashCode(exclude = "previousNumber")
public class Segment {
    private final String scope;
    @NonNull
    private final String streamName;
    private final int segmentNumber;
    private final int previousNumber;

    public Segment(String scope, String streamName, int number, int previous) {
        super();
        this.scope = scope;
        Preconditions.checkNotNull(streamName);
        Preconditions.checkArgument(streamName.matches("^\\w+\\z"), "Name must be [a-zA-Z0-9]*");
        this.streamName = streamName;
        this.segmentNumber = number;
        this.previousNumber = previous;
    }

    public String getQualifiedName() {
        return getQualifiedName(scope, streamName, segmentNumber);
    }

    public static String getQualifiedName(String scope, String streamName, int segmentNumber) {
        StringBuffer sb = new StringBuffer();
        if (scope != null) {
            sb.append(scope);
            sb.append('/');
        }
        sb.append(streamName);
        sb.append('/');
        sb.append(segmentNumber);
        return sb.toString();
    }

    /**
     * @return True if this segment is a replacement or partial replacement for the one passed.
     */
    public boolean succeeds(Segment other) {
        return ((scope == null) ? other.scope == null : scope.equals(other.scope))
                && streamName.equals(other.streamName) && previousNumber == other.segmentNumber;
    }

    public static String getScopeFromQualifiedName(String segment) {
        String[] tokens = segment.split("/");
        Preconditions.checkArgument(tokens.length >= 3);
        return tokens[0];
    }

    public static String getStreamNameFromQualifiedName(String segment) {
        String[] tokens = segment.split("/");
        Preconditions.checkArgument(tokens.length >= 3);
        return tokens[1];
    }

    public static int getSegmentNumberFromQualifiedName(String segment) {
        String[] tokens = segment.split("/");
        Preconditions.checkArgument(tokens.length >= 3);
        return Integer.parseInt(tokens[2]);
    }
}
