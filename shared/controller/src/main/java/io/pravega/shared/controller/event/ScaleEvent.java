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
package io.pravega.shared.controller.event;

import lombok.Data;

@Data
public class ScaleEvent implements ControllerEvent {
    public static final byte UP = (byte) 0;
    public static final byte DOWN = (byte) 1;
    private static final long serialVersionUID = 1L;

    private final String scope;
    private final String stream;
    private final int segmentNumber;
    private final byte direction;
    private final long timestamp;
    private final int numOfSplits;
    private final boolean silent;

    @Override
    public String getKey() {
        return String.format("%s/%s", scope, stream);
    }
}
