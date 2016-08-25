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

import com.emc.pravega.stream.Api;
import com.emc.pravega.controller.stream.api.v1.ProducerService;
import com.emc.pravega.controller.stream.api.v1.SegmentId;
import com.emc.pravega.controller.stream.api.v1.SegmentUri;
import com.emc.pravega.stream.impl.model.ModelHelper;
import org.apache.thrift.TException;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Stream Controller Producer API server implementation.
 */
public class ProducerServiceImpl implements ProducerService.Iface {

    private Api.Producer producerApi;

    public ProducerServiceImpl(Api.Producer producerApi) {
        this.producerApi = producerApi;
    }

    @Override
    public List<SegmentId> getCurrentSegments(String stream) throws TException {
        try {
            return producerApi.getCurrentSegments(stream).get().getSegments().parallelStream()
                    .map(ModelHelper::decode).collect(Collectors.toList());
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SegmentUri getURI(SegmentId id) throws TException {
        //invoke Api.ApiProducer.getURI(...)
        producerApi.getURI(id);
        return null;
    }
}
