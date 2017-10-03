package io.pravega.controller.server.eventProcessor.requesthandlers;

import io.pravega.controller.retryable.RetryableException;

public class TaskExceptions {
    public static class StreamTaskException extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

    public static class StartException extends StreamTaskException {
    }

    public static class EventPostException extends StreamTaskException implements RetryableException {
    }

    public static class RequestProcessingNotEnabledException extends StreamTaskException {
    }
}
