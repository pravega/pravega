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
package com.emc.pravega.stream;

import com.google.common.base.Preconditions;

import lombok.Data;
import lombok.NonNull;

/**
 * An identifier for a segment of a stream.
 */
@Data
public class SegmentId {
    private final String scope;
    @NonNull
    private final String name;
    private final int number;
    private final int previous;

    private final String endpoint;
    private final int port;

    public SegmentId(String scope, String name, int number, int previous, String endpoint, int port) {
        super();
        this.scope = scope;
        this.endpoint = endpoint;
        this.port = port;
        Preconditions.checkNotNull(name);
        Preconditions.checkArgument(name.matches("^\\w+\\z"), "Name must be [a-zA-Z0-9]*");
        this.name = name;
        this.number = number;
        this.previous = previous;
    }

    public String getQualifiedName() {
        StringBuffer sb = new StringBuffer();
        if (scope != null) {
            sb.append(scope);
            sb.append('/');
        }
        sb.append(name);
        sb.append('/');
        sb.append(number);
        return sb.toString();
    }

    /**
     * @return True if this segment is a replacement or partial replacement for the one passed.
     */
    public boolean succeeds(SegmentId other) {
        return ((scope == null) ? other.scope == null : scope.equals(other.scope)) && name.equals(other.name)
                && previous == other.number;
    }
}
