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
package com.emc.pravega.common.util;

import com.emc.pravega.common.netty.Append;
import com.emc.pravega.common.netty.WireCommands;
import zipkin.Annotation;
import zipkin.Endpoint;
import zipkin.Span;
import zipkin.reporter.AsyncReporter;
import zipkin.reporter.Encoding;
import zipkin.reporter.okhttp3.OkHttpSender;

import java.util.UUID;

public class ZipKinTracer implements AutoCloseable {
    private static ZipKinTracer tracer;
    private final AsyncReporter<Span> reporter;

    public static ZipKinTracer getTracer() {
        if( tracer == null ) {
            tracer = new ZipKinTracer();
        }
        return tracer;
    }

    protected ZipKinTracer() {
        reporter = AsyncReporter.builder(OkHttpSender.builder().
                endpoint("http://localhost:9411/api/v1/spans").encoding(Encoding.JSON).build()).build();
    }
    public void traceStartAppend(Append append) {
        Span span = Span.builder().name("rpc").
                id(0).
                traceId(append.getEventNumber()).
                debug(true).
                addAnnotation(Annotation.create(System.currentTimeMillis() * 1000,
                                "cs", Endpoint.create("producer", 1000)))
                .build();
        reporter.report(span);
    }

    public void traceAppendAcked(Append append) {
        Span span = Span.builder().name("rpc").
                id(0).
                traceId(append.getEventNumber()).
                debug(true).
                addAnnotation(Annotation.create(System.currentTimeMillis() * 1000,
                        "cr", Endpoint.create("producer", 1000)))
                .build();
        reporter.report(span);

    }

    @Override
    public void close() throws Exception {
        reporter.flush();
    }

    public void traceAppendReceived(Append append) {
        Span span = Span.builder().name("rpc").
                id(1).
                traceId(append.getEventNumber()).
                debug(true).
                addAnnotation(Annotation.create(System.currentTimeMillis() * 1000,
                        "sr", Endpoint.create("host", 1000)))
                .build();
        reporter.report(span);
    }

    public void traceServerAcking(long lastAcked, WireCommands.DataAppended appended) {
        for( long traced = lastAcked +1; traced <= appended.getEventNumber(); traced++ ) {
            Span span = Span.builder().name("rpc").
                    id(1).
                    traceId(traced).
                    debug(true).
                    addAnnotation(Annotation.create(System.currentTimeMillis() * 1000, "ss",
                            Endpoint.create("host", 1000))).build();
            reporter.report(span);
        }
    }

    public void traceDataFrameSerialize(UUID clientId, long eventNumber) {
  /*      Span span = Span.builder().name("df").
                id(2).
                traceId(eventNumber).
                debug(true).
                addAnnotation(Annotation.create(System.currentTimeMillis() * 1000,
                        "df", Endpoint.create("host", 1000)))
                .build();
        reporter.report(span);
    */}
}
