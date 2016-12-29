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

package com.emc.pravega.controller.server.rest.resources;

import com.emc.pravega.controller.server.rest.v1.ApiV1;
import com.emc.pravega.controller.stream.api.v1.CreateStreamStatus;
/*
import com.emc.pravega.controller.server.rpc.v1.ControllerService;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
*/
import com.emc.pravega.stream.StreamConfiguration;

import javax.ws.rs.core.Response;
import java.io.IOException;

/*
Implementation of Resource
 */
public class ResourceImpl implements ApiV1.Controller {

    /*private final StreamMetadataStore streamStore;
    private final HostControllerStore hostStore;
    private final StreamMetadataTasks streamMetadataTasks;
    private final StreamTransactionMetadataTasks streamTransactionMetadataTasks;
    private final ControllerService controllerService = new ControllerService(streamStore, hostStore, streamMetadataTasks, streamTransactionMetadataTasks);
*/
    @Override
    public Response createStream(StreamConfiguration streamConfiguration) throws IOException {
        return Response.serverError().status(500).build();
    }

    /*
    @Override
    public CreateStreamStatus createStream(final StreamConfig streamConfig) throws TException {
        return FutureHelpers.getAndHandleExceptions(controllerService.createStream(ModelHelper.encode(streamConfig),
                System.currentTimeMillis()), RuntimeException::new);
    }
     */
}
