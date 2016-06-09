package com.emc.logservice.common;

import org.slf4j.Logger;

import java.util.Random;

/**
 * Extension methods for Logger class.
 */
public class LoggerHelpers {
    private static final Random IdGenerator = new Random();

    /**
     * Traces the fact that a method entry has occurred.
     *
     * @param log    The Logger to log to.
     * @param method The name of the method.
     * @param args   The arguments to the method.
     * @return A randomly generated identifier that can be used to correlate this traceEnter with its corresponding traceLeave.
     */
    public static int traceEnter(Logger log, String method, Object... args) {
        if (!log.isTraceEnabled()) {
            return 0;
        }

        int id = IdGenerator.nextInt();
        log.trace("ENTER {}#{} {}.", method, id, args);
        return id;
    }

    /**
     * Traces the fact that a method entry has occurred.
     *
     * @param log      The Logger to log to.
     * @param objectId The id of the containing object.
     * @param method   The name of the method.
     * @param args     The arguments to the method.
     * @return A randomly generated identifier that can be used to correlate this traceEnter with its corresponding traceLeave.
     */
    public static int traceEnter(Logger log, String objectId, String method, Object... args) {
        if (!log.isTraceEnabled()) {
            return 0;
        }

        int id = IdGenerator.nextInt();
        log.trace("{}: ENTER {}#{} {}.", objectId, method, id, args);
        return id;
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
        if (args.length == 0) {
            log.trace("LEAVE {}#{}.", method, traceEnterId);
        }
        else {
            log.trace("LEAVE {}#{}: {}.", method, traceEnterId, args);
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
        if (args.length == 0) {
            log.trace("{}: LEAVE {}#{}.", objectId, method, traceEnterId);
        }
        else {
            log.trace("{}: LEAVE {}#{}: {}.", objectId, method, traceEnterId, args);
        }
    }
}