/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common;

import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;

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
        log.trace("ENTER {}@{} {}.", method, time, args);
        return time;
    }

    /**
     * Traces the fact that a method entry has occurred.
     *
     * @param log     The Logger to log to.
     * @param context Identifying context for the operation. For example, this can be used to differentiate between
     *                different instances of the same object.
     * @param method  The name of the method.
     * @param args    The arguments to the method.
     * @return A generated identifier that can be used to correlate this traceEnter with its corresponding traceLeave.
     * This is usually generated from the current System time, and when used with traceLeave it can be used to log
     * elapsed call times.
     */
    public static long traceEnterWithContext(Logger log, String context, String method, Object... args) {
        if (!log.isTraceEnabled()) {
            return 0;
        }

        long time = CURRENT_TIME.get();
        log.trace("ENTER {}::{}@{} {}.", context, method, time, args);
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
            log.trace("LEAVE {}@{} (elapsed={}us).", method, traceEnterId, ELAPSED_MICRO.apply(traceEnterId));
        } else {
            log.trace("LEAVE {}@{} {} (elapsed={}us).", method, traceEnterId, args, ELAPSED_MICRO.apply(traceEnterId));
        }
    }

    /**
     * Traces the fact that a method has exited normally.
     *
     * @param log          The Logger to log to.
     * @param context      Identifying context for the operation. For example, this can be used to differentiate between
     *                     different instances of the same object.
     * @param method       The name of the method.
     * @param traceEnterId The correlation Id obtained from a traceEnter call.
     * @param args         Additional arguments to log.
     */
    public static void traceLeave(Logger log, String context, String method, long traceEnterId, Object... args) {
        if (!log.isTraceEnabled()) {
            return;
        }

        if (args.length == 0) {
            log.trace("LEAVE {}::{}@{} (elapsed={}us).", context, method, traceEnterId, ELAPSED_MICRO.apply(traceEnterId));
        } else {
            log.trace("LEAVE {}::{}@{} {} (elapsed={}us).", context, method, traceEnterId, args, ELAPSED_MICRO.apply(traceEnterId));
        }
    }

    /**
     * Writes a debug-level log line on the provided logger consisting of a header tag (e.g., requestId) and the message
     * passed (plus the arguments). Note that the message may include formatting anchors to be filled by subsequent
     * arguments.
     *
     * @param log           The Logger to log to.
     * @param requestId     Tag used as header for the log line.
     * @param message       Message to log including formatting anchors.
     * @param args          Additional arguments to log expected to fill in the message's formatting anchors.
     */
    public static void debugLogWithTag(Logger log, long requestId, String message, Object... args) {
        log.debug("[requestId={}] ".concat(message), requestId, args);
    }

    /**
     * Writes a info-level log line on the provided logger consisting of a header tag (e.g., requestId) and the message
     * passed (plus the arguments). Note that the message may include formatting anchors to be filled by subsequent
     * arguments.
     *
     * @param log           The Logger to log to.
     * @param requestId     Tag used as header for the log line.
     * @param message       Message to log including formatting anchors.
     * @param args          Additional arguments to log expected to fill in the message's formatting anchors.
     */
    public static void infoLogWithTag(Logger log, long requestId, String message, Object... args) {
        log.info("[requestId={}] ".concat(message), requestId, args);
    }

    /**
     * Writes a warn-level log line on the provided logger consisting of a header tag (e.g., requestId) and the message
     * passed (plus the arguments). Note that the message may include formatting anchors to be filled by subsequent
     * arguments.
     *
     * @param log           The Logger to log to.
     * @param requestId     Tag used as header for the log line.
     * @param message       Message to log including formatting anchors.
     * @param args          Additional arguments to log expected to fill in the message's formatting anchors.
     */
    public static void warnLogWithTag(Logger log, long requestId, String message, Object... args) {
        log.warn("[requestId={}] ".concat(message), requestId, args);
    }

    /**
     * Writes an error-level log line on the provided logger consisting of a header tag (e.g., requestId) and the
     * message passed (plus the arguments). Note that the message may include formatting anchors to be filled by
     * subsequent arguments.
     *
     * @param log           The Logger to log to.
     * @param requestId     Tag used as header for the log line.
     * @param message       Message to log including formatting anchors.
     * @param args          Additional arguments to log expected to fill in the message's formatting anchors.
     */
    public static void errorLogWithTag(Logger log, long requestId, String message, Object... args) {
        log.error("[requestId={}] ".concat(message), requestId, args);
    }
}
