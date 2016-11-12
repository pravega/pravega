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

package com.emc.pravega.common;

import org.slf4j.Logger;

import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Extension methods for Logger class.
 */
public final class LoggerHelpers {
    /**
     * Used to generate TraceEnter Ids. To properly calculate elapsed time, this returns the current System.nanoTime().
     */
    private static final Supplier<Long> CURRENT_TIME = System::nanoTime;

    /**
     * Calculates the elapsed time (in microseconds), given a properly generated TraceEnterId.
     */
    private static final Function<Long, Long> ELAPSED_MICRO = startTime -> (CURRENT_TIME.get() - startTime) / 1000;

    /**
     * Traces the fact that a method entry has occurred.
     *
     * @param log    The Logger to log to.
     * @param method The name of the method.
     * @param args   The arguments to the method.
     * @return A generated identifier that can be used to correlate this traceEnter with its corresponding traceLeave.
     * This is usually generated from the current System time, and when used with traceLeave it can be used to log
     * elapsed call times.
     */
    public static long traceEnter(Logger log, String method, Object... args) {
        if (!log.isTraceEnabled()) {
            return 0;
        }

        long time = CURRENT_TIME.get();
        log.trace("ENTER {}#{} {}.", method, time, args);
        return time;
    }

    /**
     * Traces the fact that a method entry has occurred.
     *
     * @param log      The Logger to log to.
     * @param objectId The id of the containing object.
     * @param method   The name of the method.
     * @param args     The arguments to the method.
     * @return A generated identifier that can be used to correlate this traceEnter with its corresponding traceLeave.
     * This is usually generated from the current System time, and when used with traceLeave it can be used to log
     * elapsed call times.
     */
    public static long traceEnter(Logger log, String objectId, String method, Object... args) {
        if (!log.isTraceEnabled()) {
            return 0;
        }

        long time = CURRENT_TIME.get();
        log.trace("{}: ENTER {}#{} {}.", objectId, method, time, args);
        return time;
    }

    /**
     * Traces the fact that a method has exited normally.
     *
     * @param log          The Logger to log to.
     * @param method       The name of the method.
     * @param traceEnterId The correlation Id obtained from a traceEnter call.
     * @param args         Additional arguments to log.
     */
    public static void traceLeave(Logger log, String method, long traceEnterId, Object... args) {
        if (!log.isTraceEnabled()) {
            return;
        }

        if (args.length == 0) {
            log.trace("LEAVE {}#{} (elapsed={}us).", method, traceEnterId, ELAPSED_MICRO.apply(traceEnterId));
        } else {
            log.trace("LEAVE {}#{}: {} (elapsed={}us).", method, traceEnterId, args, ELAPSED_MICRO.apply(traceEnterId));
        }
    }

    /**
     * Traces the fact that a method has exited normally.
     *
     * @param log          The Logger to log to.
     * @param objectId     The id of the containing object.
     * @param method       The name of the method.
     * @param traceEnterId The correlation Id obtained from a traceEnter call.
     * @param args         Additional arguments to log.
     */
    public static void traceLeave(Logger log, String objectId, String method, long traceEnterId, Object... args) {
        if (!log.isTraceEnabled()) {
            return;
        }

        if (args.length == 0) {
            log.trace("{}: LEAVE {}#{} (elapsed={}us).", objectId, method, traceEnterId,
                    ELAPSED_MICRO.apply(traceEnterId));
        } else {
            log.trace("{}: LEAVE {}#{}: {} (elapsed={}us).", objectId, method, traceEnterId, args, ELAPSED_MICRO
                    .apply(traceEnterId));
        }
    }
}