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
package com.emc.pravega.controller.server.rpc.v1;

import com.emc.pravega.stream.ControllerApi;
import com.emc.pravega.controller.stream.api.v1.ConsumerService;
import com.emc.pravega.controller.stream.api.v1.Position;
import com.emc.pravega.stream.impl.model.ModelHelper;
import org.apache.thrift.TException;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Stream Controller Consumer API server implementation.
 */
public class ConsumerServiceImpl implements ConsumerService.Iface {

    private ControllerApi.Consumer consumerApi;

    public ConsumerServiceImpl(ControllerApi.Consumer consumerApi) {
        this.consumerApi = consumerApi;
    }

    @Override
    public List<Position> getPositions(String stream, long timestamp, int count) throws TException {
        //invoke Api.ApiConsumer.getPositions(...)

        try {
            // convert pravega.stream.Position to pravega.controller.stream.api.v1.Position before sending it over wire
            return consumerApi.getPositions(stream, timestamp, count).get()
                    .stream()
                    .map(ModelHelper::decode)
                    .collect(Collectors.toList());
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }

    @Override
    public List<Position> updatePositions(String stream, List<Position> positions) throws TException {

        try {
            //invoke Api.Consumer.updatePositions(...)
            // convert pravega.controller.stream.api.v1.Position to pravega.stream.Position before invoking method on server

            return consumerApi
                    .updatePositions(stream, positions.stream().map(ModelHelper::encode).collect(Collectors.toList()))
                    // convert pravega.stream.Position to pravega.controller.stream.api.v1.Position before sending it over wire
                    .get()
                    .stream()
                    .map(ModelHelper::decode)
                    .collect(Collectors.toList());
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
