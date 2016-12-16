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
package com.emc.pravega.stream.impl;

import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.StreamConfiguration;
import com.google.common.base.Preconditions;

import lombok.Getter;

/**
 * An implementation of a stream for the special case where the stream is only ever composed of one segment.
 */
public class StreamImpl implements Stream {

    @Getter
    private final String scope;
    @Getter
    private final String streamName;
    @Getter
    private final StreamConfiguration config;

    public StreamImpl(String scope, String streamName, StreamConfiguration config) {
        Preconditions.checkNotNull(scope);
        Preconditions.checkNotNull(streamName);
        Preconditions.checkNotNull(config);
        this.scope = scope;
        this.streamName = streamName;
        this.config = config;
    }

    @Override
    public String getScopedName() {
        StringBuffer sb = new StringBuffer();
        if (scope != null) {
            sb.append(scope);
            sb.append('/');
        }
        sb.append(streamName);
        return sb.toString();
    }
}
