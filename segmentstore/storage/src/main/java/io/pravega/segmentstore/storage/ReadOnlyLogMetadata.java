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
package io.pravega.segmentstore.storage;

public interface ReadOnlyLogMetadata {
    /**
     * Gets a value indicating the current epoch of this LogMetadata. This changes upon every successful log initialization,
     * immediately after it was fenced.
     *
     * @return The current epoch.
     */
    long getEpoch();

    /**
     * Gets a value indicating the current version of the Metadata (this changes upon every successful metadata persist).
     * Note: this is different from getEpoch() - which gets incremented with every successful recovery.
     *
     * @return The current version.
     */
    int getUpdateVersion();

    /**
     * Gets a value indicating whether this log is enabled or not.
     *
     * @return True if enabled, false otherwise.
     */
    boolean isEnabled();
}
