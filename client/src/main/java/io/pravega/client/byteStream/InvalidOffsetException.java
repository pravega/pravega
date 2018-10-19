/**
 * Copyright (c) 2018 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.byteStream;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * The offset provided is not valid. It is either negative or beyond the end of the segment.
 */
@RequiredArgsConstructor
public class InvalidOffsetException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    @Getter
    private final long providedOffset;
    
}
