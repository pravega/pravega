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

import com.google.common.base.Preconditions;
import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
public final class StreamImpl implements StreamInternal, Serializable {

    private static final long serialVersionUID = 1L;
    private final String scope;
    private final String streamName;

    /**
     * Creates a new instance of the Stream class.
     *
     * @param scope      The scope of the stream.
     * @param streamName The name of the stream.
     */
    public StreamImpl(String scope, String streamName) {
        Preconditions.checkNotNull(scope);
        Preconditions.checkNotNull(streamName);
        this.scope = scope;
        this.streamName = streamName;
    }
    
    private Object writeReplace() {
        return new SerializedForm(getScopedName());
    }
    
    @Data
    @AllArgsConstructor
    private static class SerializedForm implements Serializable {
        private static final long serialVersionUID = 1L;
        private String value;
        Object readResolve() {
            return StreamInternal.fromScopedName(value);
        }
    }
}
