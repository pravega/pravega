/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.contracts;

import lombok.Getter;

public class WrongHostException extends StreamSegmentException {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    @Getter
    private final String correctHost;

    public WrongHostException(String streamSegmentName, String correctHost) {
        super(streamSegmentName, "");
        this.correctHost = correctHost;
    }
}
