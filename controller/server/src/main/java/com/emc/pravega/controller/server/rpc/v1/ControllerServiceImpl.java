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
import com.emc.pravega.controller.stream.api.v1.ControllerService;
import com.emc.pravega.controller.stream.api.v1.SegmentId;
import com.emc.pravega.controller.stream.api.v1.SegmentUri;
import com.emc.pravega.controller.stream.api.v1.Status;
import com.emc.pravega.controller.stream.api.v1.StreamConfig;
import com.emc.pravega.controller.stream.api.v1.Position;
import com.emc.pravega.stream.ControllerApi;
import com.emc.pravega.stream.impl.model.ModelHelper;
import org.apache.commons.lang.NotImplementedException;
import org.apache.thrift.TException;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Stream controller RPC server implementation
 */
public class ControllerServiceImpl implements ControllerService.Iface {
    private ControllerApi.Admin adminApi;
    private ControllerApi.Consumer consumerApi;
    private ControllerApi.Producer producerApi;

    public ControllerServiceImpl(ControllerApi.Admin adminApi, ControllerApi.Consumer consumerApi, ControllerApi.Producer producerApi) {
        this.adminApi = adminApi;
        this.consumerApi = consumerApi;
        this.producerApi = producerApi;
    }

    @Override
    public Status createStream(StreamConfig streamConfig) throws TException {
        return FutureHelpers.getAndHandleExceptions(adminApi.createStream(ModelHelper.encode(streamConfig)),
                RuntimeException::new);
    }

    @Override
    public Status alterStream(StreamConfig streamConfig) throws TException {
        //invoke ControllerApi.ApiAdmin.alterStream(...)
        adminApi.alterStream(null);
        throw new NotImplementedException();
    }

    @Override
    public List<SegmentId> getCurrentSegments(String stream) throws TException {
        // TODO: fix null pointer warning because of exception = null return scenario
        return FutureHelpers.getAndHandleExceptions(producerApi.getCurrentSegments(stream), RuntimeException::new)
                .getSegments()
                .parallelStream()
                .map(ModelHelper::decode)
                .collect(Collectors.toList());
    }

    @Override
    public SegmentUri getURI(String stream, SegmentId id) throws TException {
        return ModelHelper.decode(FutureHelpers.getAndHandleExceptions(producerApi.getURI(stream, ModelHelper.encode(id)), RuntimeException::new));
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
                consumerApi.updatePositions(stream, positions.stream().map(ModelHelper::encode).collect(Collectors.toList())),
                RuntimeException::new)
                .stream()
                .map(ModelHelper::decode)
                .collect(Collectors.toList());
    }
}
