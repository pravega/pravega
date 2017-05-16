/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util;

import java.nio.ByteBuffer;

public class ByteBufferUtils {

    public static ByteBuffer slice(ByteBuffer orig, int begin, int length) {
        int pos = orig.position();
        int limit = orig.limit();
        orig.limit(begin + length);
        orig.position(begin);
        ByteBuffer result = orig.slice();
        orig.limit(limit);
        orig.position(pos);
        return result;
    }
    
    
}
