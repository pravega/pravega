/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
package io.pravega.service.server.reading;

import io.pravega.common.io.StreamHelpers;
import io.pravega.service.contracts.ReadResultEntry;
import io.pravega.service.contracts.ReadResultEntryContents;
import io.pravega.service.contracts.ReadResultEntryType;
import lombok.Getter;
import org.junit.Assert;

import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Helper class for unit tests that require usage of an AsyncReadResultProcessor to handle ReadResults.
 */
public class TestReadResultHandler implements AsyncReadResultHandler {
    @Getter
    private final AtomicReference<Throwable> error = new AtomicReference<>();
    private final ByteArrayOutputStream readContents;
    @Getter
    private final CompletableFuture<Void> completed;
    private final Duration timeout;

    public TestReadResultHandler(ByteArrayOutputStream readContents, Duration timeout) {
        this.readContents = readContents;
        this.completed = new CompletableFuture<>();
        this.timeout = timeout;
    }

    @Override
    public boolean shouldRequestContents(ReadResultEntryType entryType, long streamSegmentOffset) {
        return true;
    }

    @Override
    public boolean processEntry(ReadResultEntry e) {
        ReadResultEntryContents c = e.getContent().join();
        byte[] data = new byte[c.getLength()];
        try {
            StreamHelpers.readAll(c.getData(), data, 0, data.length);
            readContents.write(data);
            return true;
        } catch (Exception ex) {
            processError(ex);
            return false;
        }
    }

    @Override
    public void processError(Throwable cause) {
        this.error.set(cause);
        Assert.assertFalse("Result is already completed.", this.completed.isDone());
        this.completed.complete(null); // We care only that it completed, not that it completed in error.
    }

    @Override
    public void processResultComplete() {
        Assert.assertFalse("Result is already completed.", this.completed.isDone());
        this.completed.complete(null);
    }

    @Override
    public Duration getRequestContentTimeout() {
        return this.timeout;
    }
}
