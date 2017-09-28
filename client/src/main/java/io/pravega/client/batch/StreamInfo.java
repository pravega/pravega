package io.pravega.client.batch;

import io.pravega.client.stream.Stream;
import lombok.Data;

@Data
public class StreamInfo {

	private final Stream stream;
	private final long length;
	private final long creationTime;
	private final boolean isSealed;
	private final Long endTime;
	
}
