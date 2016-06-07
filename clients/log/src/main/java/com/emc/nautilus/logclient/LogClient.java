package com.emc.nautilus.logclient;

// Defines a client for the log. The actual implementation of this class will
// connect to a service & manage TCP connections.
public interface LogClient {
	// Creates a new stream with given name. Returns false if the stream already
	// existed
	boolean createLog(String name);

	// Determines whether the log exists or not.
	boolean logExists(String name);

	// Opens an existing log for writing. this operation will fail if the stream
	// does not exist
	// This operation may be called multiple times on the same log from the
	// same or different clients (i.e., there can be concurrent Stream Writers
	// in the same process space).
	LogAppender openLogForAppending(String name, SegmentOutputConfiguration config);

	// Opens an existing log for reading. This operation will fail if the
	// log does not exist.
	// This operation may be called multiple times on the same stream from the
	// same client (i.e., there can be concurrent Stream Readers in the same
	// process space).
	LogInputStream openLogForReading(String name, SegmentInputConfiguration config);
}