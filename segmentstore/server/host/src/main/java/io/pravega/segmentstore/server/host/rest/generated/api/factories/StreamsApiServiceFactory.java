package io.pravega.segmentstore.server.host.rest.generated.api.factories;

import io.pravega.segmentstore.server.host.rest.generated.api.StreamsApiService;
import io.pravega.segmentstore.server.host.rest.generated.api.impl.StreamsApiServiceImpl;


public class StreamsApiServiceFactory {
    private final static StreamsApiService service = new StreamsApiServiceImpl();

    public static StreamsApiService getStreamsApi() {
        return service;
    }
}
