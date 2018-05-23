/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import lombok.Data;

import java.io.Serializable;
import java.util.Map;


/**
 * This is data class for storing stream cut with time when the cut was computed.
 * And the size of data being cut.
 */
@Data
public class StreamCutRecord implements Serializable {
    /**
     * Time when this stream cut was recorded.
     */
    final long recordingTime;
    /**
     * Amount of data in the stream preceeding this cut.
     */
    final long recordingSize;
    /**
     * Actual Stream cut.
     */
    final Map<Long, Long> streamCut;
}
