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

package io.pravega.shared.metrics;

public interface MetricListener extends AutoCloseable {

    /**
     * An operation with the given value succeeded.
     *
     * @param operation The operation
     * @param value the value
     */
    void reportSuccessValue(String operation, long value);

    /**
     * An operation with the given value failed.
     *
     * @param operation The operation
     * @param value the value
     */
    void reportFailValue(String operation, long value);

    @Override
    void close();

}
