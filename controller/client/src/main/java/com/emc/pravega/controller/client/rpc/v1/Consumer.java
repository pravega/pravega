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
package com.emc.pravega.controller.client.rpc.v1;

import com.emc.pravega.controller.contract.v1.api.Api;
import com.emc.pravega.controller.stream.api.v1.ConsumerService;
import com.emc.pravega.stream.Position;
import org.apache.thrift.TException;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * RPC based implementation of Stream Controller Consumer V1 API
 */
public class Consumer implements Api.Consumer {
    @Override
    public CompletableFuture<List<Position>> getPositions(String stream, long timestamp, int count) {
        //Use RPC client to invoke getPositions
        ConsumerService.Client client = new ConsumerService.Client(null);
        try {
            client.getPositions(stream, timestamp, count);
        } catch (TException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public CompletableFuture<List<Position>> updatePositions(List<Position> positions) {
        //Use RPC client to invoke updatePositions
        ConsumerService.Client client = new ConsumerService.Client(null);
        try {
            client.updatePositions(null);
        } catch (TException e) {
            e.printStackTrace();
        }
        return null;
    }
}
