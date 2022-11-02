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
package io.pravega.segmentstore.storage.chunklayer;

import io.pravega.segmentstore.storage.mocks.InMemorySnapshotInfoStore;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.val;

import java.util.concurrent.CompletableFuture;

/**
 * {@link SnapshotInfoStore} implementation that fails predictably based on provided list of {@link FlakinessPredicate}.
 */
public class FlakySnapshotInfoStore extends InMemorySnapshotInfoStore {
    /**
     * {@link FlakyInterceptor} that intercepts calls and matches against given set of {@link FlakinessPredicate}.
     */
    @Getter
    private final FlakyInterceptor interceptor = new FlakyInterceptor();

    @Override
    @SneakyThrows
    public CompletableFuture<SnapshotInfo> getSnapshotId(int containerId) {
        try {
            interceptor.intercept(Integer.toString(containerId), "getSnapshotId.before");
            val retValue = super.getSnapshotId(containerId);
            interceptor.intercept(Integer.toString(containerId), "getSnapshotId.after");
            return retValue;
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    @SneakyThrows
    public CompletableFuture<Void> setSnapshotId(int containerId, SnapshotInfo checkpoint) {
        try {
            interceptor.intercept(Integer.toString(containerId), "setSnapshotId.before");
            val retValue = super.setSnapshotId(containerId, checkpoint);
            interceptor.intercept(Integer.toString(containerId), "setSnapshotId.after");
            return retValue;
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }
}
