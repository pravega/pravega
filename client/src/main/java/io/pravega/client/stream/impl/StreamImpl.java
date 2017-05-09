/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import io.pravega.client.stream.Stream;
import com.google.common.base.Preconditions;

import lombok.Data;

/**
 * An implementation of a stream for the special case where the stream is only ever composed of one segment.
 */
@Data
public class StreamImpl implements Stream {

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
