package com.emc.logservice.contracts;

import java.util.UUID;

import lombok.Data;

@Data
public class ConnectionInfo {

	private final String segment;
	private final UUID connectionId;
	private final long bytesWrittenSuccessfully;
	
}
