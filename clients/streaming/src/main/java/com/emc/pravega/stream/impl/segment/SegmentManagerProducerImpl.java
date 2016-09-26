/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.stream.impl.segment;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.util.RetriesExaustedException;
import com.emc.pravega.stream.ControllerApi;
import com.emc.pravega.stream.SegmentId;
import com.emc.pravega.stream.SegmentUri;
import com.emc.pravega.stream.Transaction.Status;
import com.emc.pravega.stream.TxFailedException;
import com.emc.pravega.stream.impl.StreamController;
import com.emc.pravega.stream.impl.netty.ConnectionFactoryImpl;
import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.NotImplementedException;

import java.util.UUID;

@Slf4j
@VisibleForTesting
public class SegmentManagerProducerImpl implements SegmentManagerProducer, StreamController {

    private final String stream;
    private final ControllerApi.Producer apiProducer;

    public SegmentManagerProducerImpl(String stream, ControllerApi.Producer apiProducer) {
        this.stream = stream;
        this.apiProducer = apiProducer;
    }

    @Override
    public SegmentOutputStream openSegmentForAppending(String name, SegmentOutputConfiguration config)
            throws SegmentSealedException {

        SegmentOutputStreamImpl result = new SegmentOutputStreamImpl(this, new ConnectionFactoryImpl(false), UUID.randomUUID(), name);
        try {
            result.getConnection();
        } catch (RetriesExaustedException e) {
            log.warn("Initial connection attempt failure. Suppressing.", e);
        }
        return result;
    }

    @Override
    public SegmentOutputStream openTransactionForAppending(String segmentName, UUID txId) {
        throw new NotImplementedException();
    }

    @Override
    public void createTransaction(String segmentName, UUID txId, long timeout) {
        throw new NotImplementedException();
    }

    @Override
    public void commitTransaction(UUID txId) throws TxFailedException {
        throw new NotImplementedException();
    }

    @Override
    public boolean dropTransaction(UUID txId) {
        throw new NotImplementedException();
    }

    @Override
    public Status checkTransactionStatus(UUID txId) {
        throw new NotImplementedException();
    }

    @Override
    public SegmentUri getEndpointForSegment(String segment) {
        return FutureHelpers.getAndHandleExceptions(apiProducer.getURI(stream, SegmentId.getSegmentNumberFromName(segment)), RuntimeException::new);
    }
}
