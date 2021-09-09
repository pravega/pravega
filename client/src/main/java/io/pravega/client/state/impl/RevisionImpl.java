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
package io.pravega.client.state.impl;

import com.google.common.base.Preconditions;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.state.Revision;
import java.io.Serializable;
import lombok.AccessLevel;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.SerializationException;

@EqualsAndHashCode
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
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

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(segment.getScopedName());
        sb.append(":");
        sb.append(offsetInSegment);
        sb.append(":");
        sb.append(eventAtOffset);
        return sb.toString();
    }

    public static Revision fromString(String scopedName) {
        String[] tokens = scopedName.split(":");
        if (tokens.length == 3) {
            return new RevisionImpl(Segment.fromScopedName(tokens[0]), Long.parseLong(tokens[1]), Integer.parseInt(tokens[2]));
        } else {
            throw new SerializationException("Not a valid segment name: " + scopedName);
        }
    }

    private Object writeReplace() {
        return new SerializedForm(toString());
    }

    @Data
    private static class SerializedForm implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String value;
        Object readResolve() {
            return Revision.fromString(value);
        }
    }
}
