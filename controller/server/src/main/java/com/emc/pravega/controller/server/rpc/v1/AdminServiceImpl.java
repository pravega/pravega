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

import com.emc.pravega.controller.contract.v1.api.Api;
import com.emc.pravega.controller.stream.api.v1.AdminService;
import com.emc.pravega.controller.stream.api.v1.Status;
import com.emc.pravega.controller.stream.api.v1.StreamConfig;
import org.apache.thrift.TException;

/**
 * Stream Controller Admin API server implementation.
 */
public class AdminServiceImpl implements AdminService.Iface {

    private Api.Admin adminApi;

    public AdminServiceImpl(Api.Admin adminApi) {
        this.adminApi = adminApi;
    }

    @Override
    public Status createStream(StreamConfig streamConfig) throws TException {
        //invoke Api.Admin.createStream(...)
        adminApi.createStream(null);
        return null;
    }

    @Override
    public Status alterStream(StreamConfig streamConfig) throws TException {
        //invoke Api.Admin.alterStream(...)
        adminApi.alterStream(null);
        return null;
    }
}
