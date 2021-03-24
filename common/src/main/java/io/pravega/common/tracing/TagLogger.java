/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.common.tracing;

import org.slf4j.Logger;
import org.slf4j.Marker;

/**
 * Wrapper class for a {@link Logger} instance that exposes all its functionality, plus a set of convenience methods to
 * properly log client requests ids for the end-to-end tracing mechanism.
 */
public class TagLogger implements Logger {

    private final Logger log;

    public TagLogger(Logger log) {
        this.log = log;
    }

    @Override
    public String getName() {
        return log.getName();
    }

    @Override
    public boolean isTraceEnabled() {
        return log.isTraceEnabled();
    }

    @Override
    public boolean isTraceEnabled(Marker marker) {
        return log.isTraceEnabled(marker);
    }

    @Override
    public void trace(String msg) {
        log.trace(msg);
    }

    @Override
    public void trace(String format, Object arg) {
        log.trace(format, arg);
    }

    @Override
    public void trace(String format, Object arg1, Object arg2) {
        log.trace(format, arg1, arg2);
    }

    @Override
    public void trace(String format, Object... arguments) {
        log.trace(format, arguments);
    }

    @Override
    public void trace(String msg, Throwable t) {
        log.trace(msg, t);
    }

    @Override
    public void trace(Marker marker, String msg) {
        log.trace(marker, msg);
    }

    @Override
    public void trace(Marker marker, String format, Object arg) {
        log.trace(marker, format, arg);
    }

    @Override
    public void trace(Marker marker, String format, Object arg1, Object arg2) {
        log.trace(marker, format, arg1, arg2);
    }

    @Override
    public void trace(Marker marker, String format, Object... argArray) {
        log.trace(marker, format, argArray);
    }

    @Override
    public void trace(Marker marker, String msg, Throwable t) {
        log.trace(marker, msg, t);
    }

    /**
     * Writes a trace-level log line on the provided logger consisting of a header tag (e.g., requestId) and the message
     * passed (plus the arguments) in the case that the request id comes from a client request (i.e., not a default one).
     * Note that the message may include formatting anchors to be filled by subsequent arguments.
     *
     * @param requestId     Tag used as header for the log line.
     * @param message       Message to log including formatting anchors.
     * @param args          Additional arguments to log expected to fill in the message's formatting anchors.
     */
    public void trace(long requestId, String message, Object... args) {
        if (requestId != RequestTag.NON_EXISTENT_ID) {
            log.trace(String.format("[requestId=%d] %s", requestId, message), args);
        } else {
            log.trace(message, args);
        }
    }

    @Override
    public boolean isDebugEnabled() {
        return log.isDebugEnabled();
    }

    @Override
    public boolean isDebugEnabled(Marker marker) {
        return log.isDebugEnabled(marker);
    }

    @Override
    public void debug(String msg) {
        log.debug(msg);
    }

    @Override
    public void debug(String format, Object arg) {
        log.debug(format, arg);
    }

    @Override
    public void debug(String format, Object arg1, Object arg2) {
        log.debug(format, arg1, arg2);
    }

    @Override
    public void debug(String format, Object... arguments) {
        log.debug(format, arguments);
    }

    @Override
    public void debug(String msg, Throwable t) {
        log.debug(msg, t);
    }

    @Override
    public void debug(Marker marker, String msg) {
        log.debug(marker, msg);
    }

    @Override
    public void debug(Marker marker, String format, Object arg) {
        log.debug(marker, format, arg);
    }

    @Override
    public void debug(Marker marker, String format, Object arg1, Object arg2) {
        log.debug(marker, format, arg1, arg2);
    }

    @Override
    public void debug(Marker marker, String format, Object... arguments) {
        log.debug(marker, format, arguments);
    }

    @Override
    public void debug(Marker marker, String msg, Throwable t) {
        log.debug(marker, msg, t);
    }

    /**
     * Writes a debug-level log line on the provided logger consisting of a header tag (e.g., requestId) and the message
     * passed (plus the arguments) in the case that the request id comes from a client request (i.e., not a default one).
     * Note that the message may include formatting anchors to be filled by subsequent arguments.
     *
     * @param requestId     Tag used as header for the log line.
     * @param message       Message to log including formatting anchors.
     * @param args          Additional arguments to log expected to fill in the message's formatting anchors.
     */
    public void debug(long requestId, String message, Object... args) {
        if (requestId != RequestTag.NON_EXISTENT_ID) {
            log.debug(String.format("[requestId=%d] %s", requestId, message), args);
        } else {
            log.debug(message, args);
        }
    }

    @Override
    public boolean isInfoEnabled() {
        return log.isInfoEnabled();
    }

    @Override
    public boolean isInfoEnabled(Marker marker) {
        return log.isInfoEnabled(marker);
    }

    @Override
    public void info(String msg) {
        log.info(msg);
    }

    @Override
    public void info(String format, Object arg) {
        log.info(format, arg);
    }

    @Override
    public void info(String format, Object arg1, Object arg2) {
        log.info(format, arg1, arg2);
    }

    @Override
    public void info(String format, Object... arguments) {
        log.info(format, arguments);
    }

    @Override
    public void info(String msg, Throwable t) {
        log.info(msg, t);
    }

    @Override
    public void info(Marker marker, String msg) {
        log.info(marker, msg);
    }

    @Override
    public void info(Marker marker, String format, Object arg) {
        log.info(marker, format, arg);
    }

    @Override
    public void info(Marker marker, String format, Object arg1, Object arg2) {
        log.info(marker, format, arg1, arg2);
    }

    @Override
    public void info(Marker marker, String format, Object... arguments) {
        log.info(marker, format, arguments);
    }

    @Override
    public void info(Marker marker, String msg, Throwable t) {
        log.info(marker, msg, t);
    }

    /**
     * Writes an info-level log line on the provided logger consisting of a header tag (e.g., requestId) and the message
     * passed (plus the arguments) in the case that the request id comes from a client request (i.e., not a default one).
     * Note that the message may include formatting anchors to be filled by subsequent arguments.
     *
     * @param requestId     Tag used as header for the log line.
     * @param message       Message to log including formatting anchors.
     * @param args          Additional arguments to log expected to fill in the message's formatting anchors.
     */
    public void info(long requestId, String message, Object... args) {
        if (requestId != RequestTag.NON_EXISTENT_ID) {
            log.info(String.format("[requestId=%d] %s", requestId, message), args);
        } else {
            log.info(message, args);
        }
    }

    @Override
    public boolean isWarnEnabled() {
        return log.isWarnEnabled();
    }

    @Override
    public boolean isWarnEnabled(Marker marker) {
        return log.isWarnEnabled(marker);
    }

    @Override
    public void warn(String msg) {
        log.warn(msg);
    }

    @Override
    public void warn(String format, Object arg) {
        log.warn(format, arg);
    }

    @Override
    public void warn(String format, Object... arguments) {
        log.warn(format, arguments);
    }

    @Override
    public void warn(String format, Object arg1, Object arg2) {
        log.warn(format, arg1, arg2);
    }

    @Override
    public void warn(String msg, Throwable t) {
        log.warn(msg, t);
    }

    @Override
    public void warn(Marker marker, String msg) {
        log.warn(marker, msg);
    }

    @Override
    public void warn(Marker marker, String format, Object arg) {
        log.warn(marker, format, arg);
    }

    @Override
    public void warn(Marker marker, String format, Object arg1, Object arg2) {
        log.warn(marker, format, arg1, arg2);
    }

    @Override
    public void warn(Marker marker, String format, Object... arguments) {
        log.warn(marker, format, arguments);
    }

    @Override
    public void warn(Marker marker, String msg, Throwable t) {
        log.warn(marker, msg, t);
    }

    /**
     * Writes a warn-level log line on the provided logger consisting of a header tag (e.g., requestId) and the message
     * passed (plus the arguments) in the case that the request id comes from a client request (i.e., not a default one).
     * Note that the message may include formatting anchors to be filled by subsequent arguments.
     *
     * @param requestId     Tag used as header for the log line.
     * @param message       Message to log including formatting anchors.
     * @param args          Additional arguments to log expected to fill in the message's formatting anchors.
     */
    public void warn(long requestId, String message, Object... args) {
        if (requestId != RequestTag.NON_EXISTENT_ID) {
            log.warn(String.format("[requestId=%d] %s", requestId, message), args);
        } else {
            log.warn(message, args);
        }
    }

    @Override
    public boolean isErrorEnabled() {
        return log.isErrorEnabled();
    }

    @Override
    public boolean isErrorEnabled(Marker marker) {
        return log.isErrorEnabled(marker);
    }

    @Override
    public void error(String msg) {
        log.error(msg);
    }

    @Override
    public void error(String format, Object arg) {
        log.error(format, arg);
    }

    @Override
    public void error(String format, Object arg1, Object arg2) {
        log.error(format, arg1, arg2);
    }

    @Override
    public void error(String format, Object... arguments) {
        log.error(format, arguments);
    }

    @Override
    public void error(String msg, Throwable t) {
        log.error(msg, t);
    }

    @Override
    public void error(Marker marker, String msg) {
        log.error(marker, msg);
    }

    @Override
    public void error(Marker marker, String format, Object arg) {
        log.error(marker, format, arg);
    }

    @Override
    public void error(Marker marker, String format, Object arg1, Object arg2) {
        log.error(marker, format, arg1, arg2);
    }

    @Override
    public void error(Marker marker, String format, Object... arguments) {
        log.error(marker, format, arguments);
    }

    @Override
    public void error(Marker marker, String msg, Throwable t) {
        log.error(marker, msg, t);
    }

    /**
     * Writes an error-level log line on the provided logger consisting of a header tag (e.g., requestId) and the message
     * passed (plus the arguments) in the case that the request id comes from a client request (i.e., not a default one).
     * Note that the message may include formatting anchors to be filled by subsequent arguments.
     *
     * @param requestId     Tag used as header for the log line.
     * @param message       Message to log including formatting anchors.
     * @param args          Additional arguments to log expected to fill in the message's formatting anchors.
     */
    public void error(long requestId, String message, Object... args) {
        if (requestId != RequestTag.NON_EXISTENT_ID) {
            log.error(String.format("[requestId=%d] %s", requestId, message), args);
        } else {
            log.error(message, args);
        }
    }
}
