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
 * The event router which is used to extract the routing key for pravega from the event.
 *
 * @param <T> The type of the event.
 */
public interface PravegaEventRouter<T> extends Serializable {
    /**
     * Fetch the routing key for the given event.
     *
     * @param event The type of the event.
     * @return  The routing key which will be used by the pravega writer.
     */
    String getRoutingKey(T event);
}
