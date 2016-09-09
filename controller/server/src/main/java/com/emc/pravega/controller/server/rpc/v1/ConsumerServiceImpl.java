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

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.controller.stream.api.v1.SegmentId;
import com.emc.pravega.controller.stream.api.v1.SegmentUri;
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
        // TODO: handle npe with null exception return case
        return FutureHelpers.getAndHandleExceptions(consumerApi.getPositions(stream, timestamp, count),
                RuntimeException::new)
                .parallelStream()
                .map(ModelHelper::decode)
                .collect(Collectors.toList());
    }

    @Override
    public List<Position> updatePositions(String stream, List<Position> positions) throws TException {
        // TODO: handle npe with null exception return case
        return FutureHelpers.getAndHandleExceptions(
                consumerApi.updatePositions(stream, positions.stream().map(ModelHelper::encode).collect(Collectors.toList()))
                , RuntimeException::new)
                .stream()
                .map(ModelHelper::decode)
                .collect(Collectors.toList());
    }

    @Override
    public SegmentUri getURI(SegmentId id) throws TException {
        return ModelHelper.decode(FutureHelpers.getAndHandleExceptions(consumerApi.getURI(ModelHelper.encode(id)), RuntimeException::new));
    }
}