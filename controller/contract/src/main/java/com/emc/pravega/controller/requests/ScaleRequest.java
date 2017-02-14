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
package com.emc.pravega.controller.requests;

import lombok.Data;

@Data
public class ScaleRequest implements ControllerRequest {
    public static final byte UP = (byte) 0;
    public static final byte DOWN = (byte) 1;

    private final String scope;
    private final String stream;
    private final int segmentNumber;
    private final byte direction;
    private final long timestamp;
    private final int numOfSplits;
    private final boolean silent;

    @Override
    public RequestType getType() {
        return RequestType.ScaleRequest;
    }

    @Override
    public String getKey() {
        return String.format("%s/%s", scope, stream);
    }
}
