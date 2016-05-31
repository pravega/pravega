package com.emc.logservice.contracts;

import lombok.Getter;

public class WrongHostException extends StreamSegmentException {
	@Getter
    private final String correctHost;
    

    public WrongHostException(String streamSegmentName, String correctHost) {
    	super(streamSegmentName,"");
		this.correctHost = correctHost;
    }


}
