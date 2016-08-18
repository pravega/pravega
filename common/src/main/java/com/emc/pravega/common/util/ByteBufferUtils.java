package com.emc.pravega.common.util;

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
