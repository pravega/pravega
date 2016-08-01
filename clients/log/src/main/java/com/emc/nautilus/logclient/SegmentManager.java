/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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