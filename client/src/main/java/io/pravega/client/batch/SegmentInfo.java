package io.pravega.client.batch;

import io.pravega.client.segment.impl.Segment;
import lombok.Data;

@Data
public class SegmentInfo {

	private final Segment segment;
	private final int epoch;
	private final long length;
	private final long creationTime;
	private final boolean isSealed;
	private final Long endTime;
	
}
