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

package io.pravega.stream;

import com.google.common.base.Strings;

import java.io.Serializable;

import lombok.Data;
import lombok.NonNull;

/**
 * An identifier for a segment of a stream.
 */
@Data
public class Segment implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String scope;
    @NonNull
    private final String streamName;
    private final int segmentNumber;

    /**
     * Creates a new instance of Segment class.
     *
     * @param scope      The scope string the segment belongs to.
     * @param streamName The stream name that the segment belongs to.
     * @param number     ID number for the segment.
     */
    public Segment(String scope, String streamName, int number) {
        this.scope = scope;
        this.streamName = streamName;
        this.segmentNumber = number;
    }

    public String getScopedStreamName() {
        StringBuffer sb = new StringBuffer();
        if (scope != null) {
            sb.append(scope);
            sb.append('/');
        }
        sb.append(streamName);
        return sb.toString();
    }

    public String getScopedName() {
        return getScopedName(scope, streamName, segmentNumber);
    }

    public static String getScopedName(String scope, String streamName, int segmentNumber) {
        StringBuffer sb = new StringBuffer();
        if (!Strings.isNullOrEmpty(scope)) {
            sb.append(scope);
            sb.append('/');
        }
        sb.append(streamName);
        sb.append('/');
        sb.append(segmentNumber);
        return sb.toString();
    }

    /**
     * Parses fully scoped name, and extracts the segment name only.
     *
     * @param qualifiedName Fully scoped segment name
     * @return Segment name.
     */
    public static Segment fromScopedName(String qualifiedName) {
        String[] tokens = qualifiedName.split("[/#]");
        if (tokens.length == 2) {
            return new Segment(null, tokens[0], Integer.parseInt(tokens[1]));
        } else if (tokens.length >= 3) {
            return new Segment(tokens[0], tokens[1], Integer.parseInt(tokens[2]));
        } else {
            throw new IllegalArgumentException("Not a valid segment name");
        }
    }
}
