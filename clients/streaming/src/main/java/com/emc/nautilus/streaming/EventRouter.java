package com.emc.nautilus.streaming;

public interface EventRouter {

	SegmentId getSegmentForEvent(Stream stream, String routingKey);

}
