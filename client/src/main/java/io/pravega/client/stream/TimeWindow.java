/**
 * Copyright (c) 2018 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream;

import lombok.Data;

/**
 * Represents a time window for the events which are currently being read by a reader.
 * 
 * The upper time bound is a timestamp which is greater than or equal to any that were provided by
 * any writer via the {@link EventStreamWriter#noteTime(long)} API prior to the current location in
 * the stream.
 * 
 * Similarly the lower time bound is a timestamp which is less than or equal to the most recent
 * value provided via the {@link EventStreamWriter#noteTime(long)} API for by any writer using that
 * API at the current location in the stream.
 *
 * upperTimeBound will always be greater than or equal to lowerTimeBound.
 */
@Data
public class TimeWindow {
    private final long lowerTimeBound;
    private final long upperTimeBound;
}
