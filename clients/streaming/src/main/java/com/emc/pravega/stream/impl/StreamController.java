package com.emc.pravega.stream.impl;

public interface StreamController {

    /**
     * Given a segment return the endpoint that currently is the owner of that segment.
     * 
     * The result of this function can be cached until the endpoint is unreachable or indicates it
     * is no longer the owner.
     */
    String getEndpointForSegment(String segment);
    
}
