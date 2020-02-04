/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.admin;

import com.google.common.annotations.Beta;
import io.pravega.client.stream.StreamCut;
import lombok.Data;

/**
 * This class is used to represent Stream information. It currently includes:
 *
 *  <ul>
 *  <li> scope of stream. </li>
 *  <li> name of stream. </li>
 *  <li> {@link StreamCut} which represents the current TAIL of the stream. </li>
 *  <li> {@link StreamCut} which represents the current HEAD of the stream. </li>
 *  </ul>
 */
@Beta
@Data
public class StreamInfo {
    /**
     * Scope name of the stream.
     */
    private final String scope;

    /**
     * Stream name.
     */
    private final String streamName;

    /**
     * {@link StreamCut} representing the current TAIL of the stream.
     */
    private final StreamCut tailStreamCut;

    /**
     * {@link StreamCut} representing the current HEAD of the stream.
     */
    private final StreamCut headStreamCut;
}
