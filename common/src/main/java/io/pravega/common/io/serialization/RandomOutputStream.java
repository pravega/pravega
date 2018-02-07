/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.io.serialization;

import java.io.IOException;
import java.io.OutputStream;

public interface RandomOutputStream {
    void write(int byteValue, int streamPosition) throws IOException;

    void write(byte[] buffer, int bufferOffset, int length, int streamPosition) throws IOException;

    OutputStream subStream(int streamPosition, int length);

    int size();
}
