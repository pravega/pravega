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
package com.emc.pravega.stream.impl;

import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Baseclient which handles the details of Thrift Client
 */
@Slf4j
public class BaseClient implements AutoCloseable {

    private static final int DEFAULT_SOCKET_CONNECTION_TIMEOUT_MS = 60 * 1000;
    protected ExecutorService service = Executors.newCachedThreadPool();

    private final TSocket transport;

    private final String hostName;
    private final int port;
    private final String serviceName;

    protected BaseClient(final String host, final int portNum, final String service) {
        transport = new TSocket(host, portNum, DEFAULT_SOCKET_CONNECTION_TIMEOUT_MS);
        hostName = host;
        port = portNum;
        serviceName = service;
        try {
            transport.open();
        } catch (TTransportException e) {
            log.error("Exception while opening TSocket", e);
            throw new RuntimeException("Error while opening Socket on host");
        }
    }

    protected TProtocol getTProtocol() {
        return new TMultiplexedProtocol(new TBinaryProtocol(transport), serviceName);
    }

    @Override
    public void close() throws Exception {
        transport.close();
    }
}
