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

package io.pravega.connectors.flink;

import java.io.Serializable;

/**
 * The supported modes of operation for flink's pravega writer.
 * The different modes correspond to different guarantees for the write operations that the implementation provides.
 */
public enum PravegaWriterMode implements Serializable {
    /*
     * Any write failures will be ignored hence there could be data loss.
     */
    BEST_EFFORT,

    /*
     * The writer will guarantee that all events are persisted in pravega.
     * There could be duplicate events written though.
     */
    ATLEAST_ONCE
}
