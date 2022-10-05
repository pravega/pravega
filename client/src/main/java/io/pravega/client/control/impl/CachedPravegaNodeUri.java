/**
 * Copyright Pravega Authors.
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
package io.pravega.client.control.impl;

import io.pravega.common.Timer;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import lombok.Data;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.concurrent.CompletableFuture;

/**
 * This is used in caching the segment endpoint information,
 * to avoid triggering duplicate calls in order to fetch the endpoint details from server.
 */
@Data
@RequiredArgsConstructor
public class CachedPravegaNodeUri {

    public final static int MAX_BACKOFF_MILLIS = 20000;

    @NonNull
    private final Timer timer;

    @NonNull
    private final CompletableFuture<PravegaNodeUri> pravegaNodeUri;

}
