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
import com.emc.pravega.stream.ControllerApi;
import com.emc.pravega.controller.stream.api.v1.AdminService;
import com.emc.pravega.controller.stream.api.v1.Status;
import com.emc.pravega.controller.stream.api.v1.StreamConfig;
import com.emc.pravega.stream.impl.model.ModelHelper;
import org.apache.commons.lang.NotImplementedException;
import org.apache.thrift.TException;

/**
 * Stream Controller Admin API server implementation.
 */
public class AdminServiceImpl implements AdminService.Iface {

    private ControllerApi.Admin adminApi;

    public AdminServiceImpl(ControllerApi.Admin adminApi) {
        this.adminApi = adminApi;
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
}
