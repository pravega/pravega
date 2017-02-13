/**
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.emc.pravega.common.io;

import java.io.ByteArrayOutputStream;

import com.emc.pravega.common.util.ByteArraySegment;

/**
 * A ByteArrayOutputStream that exposes the contents as a ByteArraySegment, without requiring a memory copy.
 */
public class EnhancedByteArrayOutputStream extends ByteArrayOutputStream {
    /**
     * Returns a readonly ByteArraySegment wrapping the current buffer of the ByteArrayOutputStream.
     * @return
     */
    public ByteArraySegment getData() {
        return new ByteArraySegment(this.buf, 0, this.count, true);
    }
}
