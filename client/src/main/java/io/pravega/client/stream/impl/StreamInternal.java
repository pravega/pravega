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

import io.pravega.client.stream.Stream;
import io.pravega.shared.NameUtils;

public interface StreamInternal extends Stream {

    /**
     * Gets the scoped name of this stream.
     *
     * @return String a fully scoped stream name
     */
    @Override
    default String getScopedName() {
        return NameUtils.getScopedStreamName(getScope(), getStreamName());
    }
    
    static Stream fromScopedName(String scopedName) {
        String[] tokens = scopedName.split("/");
        if (tokens.length == 2) {
            return new StreamImpl(tokens[0], tokens[1]);
        } else {
            throw new IllegalArgumentException("Not a valid segment name");
        }
    }
    
}
