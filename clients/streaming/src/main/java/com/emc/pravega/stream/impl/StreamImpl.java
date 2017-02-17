/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.stream.Stream;
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
