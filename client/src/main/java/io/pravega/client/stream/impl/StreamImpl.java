/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import com.google.common.base.Preconditions;
import java.io.ObjectStreamException;
import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
public class StreamImpl implements StreamInternal, Serializable {

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
    
    private Object writeReplace() throws ObjectStreamException {
        return new SerializedForm(getScopedName());
    }
    
    @Data
    @AllArgsConstructor
    private static class SerializedForm implements Serializable {
        private static final long serialVersionUID = 1L;
        private String value;
        Object readResolve() throws ObjectStreamException {
            return StreamInternal.fromScopedName(value);
        }
    }
}
