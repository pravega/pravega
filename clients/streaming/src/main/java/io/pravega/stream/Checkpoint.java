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
package io.pravega.stream;

import io.pravega.stream.impl.CheckpointImpl;

import java.io.Serializable;

public interface Checkpoint extends Serializable {

    /**
     * Returns the name of the Checkpoint specified in {@link ReaderGroup#initiateCheckpoint(String, java.util.concurrent.ScheduledExecutorService)}.
     * @return The checkpoint name;
     */
    String getName();
    
    /**
     * For internal use. Do not call.
     * @return This
     */
    CheckpointImpl asImpl();
}
