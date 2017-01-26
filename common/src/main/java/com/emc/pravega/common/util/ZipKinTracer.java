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
import lombok.extern.slf4j.Slf4j;
import zipkin.Annotation;
import zipkin.Endpoint;
import zipkin.Span;
import zipkin.reporter.AsyncReporter;
import zipkin.reporter.Encoding;
import zipkin.reporter.okhttp3.OkHttpSender;

import java.util.UUID;

@Slf4j
public class ZipKinTracer implements AutoCloseable {
    private static ZipKinTracer tracer;
    private static String zipkinEndpoint = "http://localhost:9411/api/v1/spans";
    private static boolean enabled = false;

    private AsyncReporter<Span> reporter;


    private ZipKinTracer() {
        reporter = AsyncReporter.builder(OkHttpSender.builder().
                endpoint(zipkinEndpoint).encoding(Encoding.JSON).build()).build();
    }

    public static ZipKinTracer getTracer() {
        if ( tracer == null ) {
            tracer = new ZipKinTracer();
        }
        return tracer;
    }

    public static void setZipkinEndpoint(String zipkinEndpoint) {
        ZipKinTracer.zipkinEndpoint = zipkinEndpoint;
    }

    public static boolean getEnable() {
        return enabled;
    }

    public void traceStartAppend(Append append) {
        if (enabled) {
            log.trace("Tracing append {}", append.getEventNumber());
            Span span = Span.builder().name("rpc").
                    id(0).
                    traceId(Math.abs(append.getConnectionId().hashCode() << 32) + append.getEventNumber()).
                    debug(true).
                    addAnnotation(Annotation.create(System.currentTimeMillis() * 1000, "cs",
                            Endpoint.create("producer", 1000))).build();
            reporter.report(span);
        }
    }

    public void traceAppendAcked(Append append) {
       if ( enabled ) {
           log.trace("Tracking ack {}", append.getEventNumber());
           Span span = Span.builder().name("rpc").
                   id(0).
                   traceId(Math.abs(append.getConnectionId().hashCode() << 32) + append.getEventNumber()).
                   debug(true).
                   addAnnotation(Annotation.create(System.currentTimeMillis() * 1000, "cr",
                           Endpoint.create("producer", 1000))).build();
           reporter.report(span);
       }
    }

    @Override
    public void close() throws Exception {
       if ( enabled ) {
           reporter.flush();
       }
    }

    public void traceAppendReceived(Long lastAcked, Append append) {
       if ( enabled ) {
           for (long traced = lastAcked + 1; traced <= append.getEventNumber(); traced++) {
               log.trace("Tracking server receive {}", traced);
               Span span = Span.builder().name("rpc").
                       id(1).
                       traceId(Math.abs(append.getConnectionId().hashCode() << 32) + traced).
                       debug(true).
                       addAnnotation(Annotation.create(System.currentTimeMillis() * 1000, "sr",
                               Endpoint.create("host", 1000))).build();
               reporter.report(span);
           }
       }
    }

    public void traceServerAcking(long lastAcked, WireCommands.DataAppended appended) {
       if ( enabled ) {
           for (long traced = lastAcked + 1; traced <= appended.getEventNumber(); traced++) {
               log.trace("Tracking server acking {}", traced);
               Span span = Span.builder().name("dl").
                       id(2).
                       traceId(Math.abs(appended.getConnectionId().hashCode() << 32) + traced).
                       debug(true).
                       addAnnotation(Annotation.create(System.currentTimeMillis() * 1000, "dr",
                               Endpoint.create("dl", 1000))).build();
               reporter.report(span);
               span = Span.builder().name("rpc").
                       id(1).
                       traceId(Math.abs(appended.getConnectionId().hashCode() << 32) + traced).
                       debug(true).
                       addAnnotation(Annotation.create(System.currentTimeMillis() * 1000, "ss",
                               Endpoint.create("host", 1000))).build();
               reporter.report(span);

           }
       }
    }

    public void traceDataFrameSerialize(UUID clientId, long lastStartedSeqNo, long eventNumber) {
       if ( enabled ) {
           for (long traced = lastStartedSeqNo + 1; traced <= eventNumber; traced++) {
               log.trace("Tracking data frame serialized for {}", traced);
               Span span = Span.builder().name("dl").
                       id(2).
                       traceId(Math.abs(clientId.hashCode() << 32) + traced).
                       debug(true).
                       addAnnotation(Annotation.create(System.currentTimeMillis() * 1000, "ds",
                               Endpoint.create("dl", 1000))).build();
               reporter.report(span);
           }
       }
    }

    public static void enableZipkin(boolean zipkinEnabled) {
       enabled = zipkinEnabled;
    }

    public void traceDLActions(long startTimeMillis, long sequence) {
        if ( enabled ) {
            Span span = Span.builder().name("distribtedlog").
                    id(0).
                    traceId(sequence).
                    debug(true).
                    addAnnotation(Annotation.create(startTimeMillis * 1000, "ls",
                            Endpoint.create("distributedlog", 1000))).build();
            reporter.report(span);

            span = Span.builder().name("distribtedlog").
                    id(0).
                    traceId(sequence).
                    debug(true).
                    addAnnotation(Annotation.create(System.currentTimeMillis() * 1000, "lr",
                            Endpoint.create("distributedlog", 1000))).build();
            reporter.report(span);

        }
    }
}
