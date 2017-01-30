package com.emc.pravega.controller.requests;

import java.time.Duration;

public class RequestStreamConstants {
    public static final String SCOPE = "pravega";
    public static final String REQUEST_STREAM = "request";
    public static final String READER_GROUP = "controllers";

    public static final long VALIDITY_PERIOD = Duration.ofMinutes(10).toMillis();
}
