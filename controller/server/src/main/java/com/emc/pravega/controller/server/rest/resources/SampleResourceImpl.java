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

import javax.ws.rs.core.Response;
import java.io.IOException;

public class SampleResourceImpl implements com.emc.pravega.controller.server.rest.v1.ApiV1.Hello {

    @Override
    public String helloWorld() {
        return "Hello World !!";
    }

    @Override
    public String printName(String name) {
        return "hello " + name;
    }

    @Override
    public Response getWrapper() {
        return Response.ok(new Object() {
            public String data = "hello world";
        }).build();
    }

    @Override
    public Response getResponse(SampleRequest request) throws IOException {
        /*ObjectMapper objectMapper = new ObjectMapper();
        SampleRequest sampleRequest = objectMapper.readValue(request, SampleRequest.class);*/
        SampleResponse sampleResponse = new SampleResponse();
        sampleResponse.setText(request.getName() + " " + String.valueOf(request.getAge()));
        return Response.ok(sampleResponse).build();
    }

}
