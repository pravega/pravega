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

package com.emc.pravega.controller.contract.v1.api;

import com.emc.pravega.controller.stream.api.v1.SegmentId;
import com.emc.pravega.controller.stream.api.v1.Status;
import com.emc.pravega.stream.Position;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.StreamSegments;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Stream Controller APIs.
 */
public final class Api {

    public static interface Admin {
        CompletableFuture<Status> createStream(StreamConfiguration streamConfig);

        CompletableFuture<Status> alterStream(StreamConfiguration streamConfig);
    }

    public static interface Producer {
        CompletableFuture<List<StreamSegments>> getCurrentSegments(String stream);

        CompletableFuture<URI> getURI(SegmentId id);
    }

    public static interface Consumer {
        CompletableFuture<List<Position>> getPositions(String stream, long timestamp, int count);

        CompletableFuture<List<Position>> updatePositions(List<Position> positions);
    }

    //Note: this is not a public interface TODO: Set appropriate scope
    static interface Host {
        //Placeholder for APIs that pravega host shall call into
    }
}
