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

/**
 * Defines a Factory for DataFrameLogs.
 */
public interface DurableDataLogFactory extends AutoCloseable {
    /**
     * Creates a new instance of a DurableDataLog class.
     *
     * @param containerId The Id of the StreamSegmentContainer for the DurableDataLog.
     */
    DurableDataLog createDurableDataLog(int containerId);

    /**
     * Initializes the DurableDataLogFactory.
     *
     * @throws DurableDataLogException If an exception occurred. The causing exception is usually wrapped in this one.
     */
    void initialize() throws DurableDataLogException;

    @Override
    void close();
}
