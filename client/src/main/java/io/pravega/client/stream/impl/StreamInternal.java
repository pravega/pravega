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

import io.pravega.client.stream.Stream;

public interface StreamInternal extends Stream {

    /**
     * Gets the scoped name of this stream.
     *
     * @return String a fully scoped stream name
     */
    @Override
    default String getScopedName() {
        StringBuffer sb = new StringBuffer();
        String scope = getScope();
        if (scope != null) {
            sb.append(scope);
            sb.append('/');
        }
        sb.append(getStreamName());
        return sb.toString();
    }
    
    public static Stream fromScopedName(String scopedName) {
        String[] tokens = scopedName.split("/");
        if (tokens.length == 2) {
            return new StreamImpl(tokens[0], tokens[1]);
        } else {
            throw new IllegalArgumentException("Not a valid segment name");
        }
    }
    
}
