package com.emc.logservice.contracts;

import java.util.UUID;

public interface ConnectionInfo {

	String getSegment();
	UUID getConnectionId();
	long getBytesWrittenSuccessfully();
	
}
