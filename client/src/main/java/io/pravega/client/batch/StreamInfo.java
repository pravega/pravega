/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.batch;

import com.google.common.annotations.Beta;
import io.pravega.client.stream.StreamCut;
import lombok.Data;

/**
 * This class is used to represent Stream information. It currently includes
 *  - scope of stream.
 *  - name of stream.
 *  - {@link StreamCut} which represents the current TAIL of the stream.
 *
 *  In future this will include the following:
 *  - isSealed
 *  - length of the stream.
 *  - creation time of the stream.
 */
@Beta
@Data
public class StreamInfo {
    private final String scope;
    private final String streamName;
    private final StreamCut currentTailStreamCut;
}
