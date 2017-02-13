/**
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.emc.pravega.stream.impl;

import java.util.concurrent.CompletableFuture;

import lombok.Data;

/**
 * This is a internal wrapper object used in the writer to pass along the routing key and the future with the actual
 * event durring write.
 * 
 * @param <Type> The type of event the client is producing
 */
@Data
public class PendingEvent<Type> {
    private final Type value;
    private final String routingKey;
    private final CompletableFuture<Boolean> ackFuture;
}
