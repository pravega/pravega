package com.emc.pravega.stream.impl;

public interface StreamController {

    String getEndpointForSegment(String segment);
    
}
