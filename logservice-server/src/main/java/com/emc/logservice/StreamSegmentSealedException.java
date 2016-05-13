package com.emc.logservice;

/**
 * Created by padura on 4/17/16.
 */
public class StreamSegmentSealedException extends StreamingException {
    public StreamSegmentSealedException(String streamName) {
        super(String.format("Stream '%s' is closed for appends.", streamName));
    }
}
