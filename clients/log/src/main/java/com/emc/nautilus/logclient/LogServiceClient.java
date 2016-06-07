package com.emc.nautilus.logclient;

import java.util.UUID;

//Defines a client for the log. The actual implementation of this class will connect to a service & manage TCP connections.
public interface LogServiceClient {
	// Creates a new stream with given name. Returns false if the stream already
	// existed
	boolean createSegment(String name);

	// Determines whether the log exists or not.
	boolean segmentExists(String name);

	SegmentOutputStream openTransactionForAppending(String name, UUID txId);
	
	// Opens an existing log for writing. this operation will fail if the stream does not exist
	// This operation may be called multiple times on the same log from the
	// same or different clients (i.e., there can be concurrent Stream Writers
	// in the same process space).
	SegmentOutputStream openSegmentForAppending(String name, SegmentOutputConfiguration config);

	// Opens an existing log for reading. This operation will fail if the
	// log does not exist.
	// This operation may be called multiple times on the same stream from the
	// same client (i.e., there can be concurrent Stream Readers in the same
	// process space).
	SegmentInputStream openLogForReading(String name, SegmentInputConfiguration config);
}