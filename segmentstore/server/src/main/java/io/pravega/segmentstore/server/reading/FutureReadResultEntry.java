/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.segmentstore.server.reading;

import io.pravega.common.function.Callbacks;
import io.pravega.common.util.BufferView;
import io.pravega.segmentstore.contracts.ReadResultEntryType;
import java.util.function.Consumer;
import javax.annotation.concurrent.GuardedBy;

/**
 * Read Result Entry for data that is not yet available in the StreamSegment (for an offset that is beyond the
 * StreamSegment's Length)
 */
class FutureReadResultEntry extends ReadResultEntryBase {
    @GuardedBy("this")
    private Consumer<FutureReadResultEntry> onCompleteOrFail;

    /**
     * Creates a new instance of the FutureReadResultEntry class.
     *
     * @param streamSegmentOffset The offset in the StreamSegment that this entry starts at.
     * @param requestedReadLength The maximum number of bytes requested for read.
     * @throws IllegalArgumentException If type is not ReadResultEntryType.Future or ReadResultEntryType.Storage.
     */
    FutureReadResultEntry(long streamSegmentOffset, int requestedReadLength) {
        super(ReadResultEntryType.Future, streamSegmentOffset, requestedReadLength);
    }

    /**
     * Registers a callback that will be invoked every time {@link #complete} or {@link #fail} is invoked.
     *
     * @param callback A {@link Consumer<FutureReadResultEntry>} to invoke. The argument will be this instance.
     */
    synchronized void setOnCompleteOrFail(Consumer<FutureReadResultEntry> callback) {
        this.onCompleteOrFail = callback;
    }

    @Override
    protected void complete(BufferView readResultEntryContents) {
        super.complete(readResultEntryContents);
        invokeWhenCompleteOrFail();
    }

    @Override
    public void fail(Throwable exception) {
        super.fail(exception);
        invokeWhenCompleteOrFail();
    }

    private void invokeWhenCompleteOrFail() {
        Consumer<FutureReadResultEntry> callback;
        synchronized (this) {
            callback = this.onCompleteOrFail;
        }

        if (callback != null) {
            Callbacks.invokeSafely(callback, this, null);
        }
    }
}
