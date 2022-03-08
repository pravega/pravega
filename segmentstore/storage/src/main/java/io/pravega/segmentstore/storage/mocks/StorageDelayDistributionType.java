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
package io.pravega.segmentstore.storage.mocks;

/**
 * Type of Distribution to use for the working of LTS.
 */
public enum StorageDelayDistributionType {
    /**
     * Uses {@link io.pravega.segmentstore.storage.mocks.SlowDelaySuppliers}.
     */
    FIXED_DISTRIBUTION_TYPE,

    /**
     * Uses {@link io.pravega.segmentstore.storage.mocks.SlowDelaySuppliers}.
     */
    NORMAL_DISTRIBUTION_TYPE,

    /**
     * Uses {@link io.pravega.segmentstore.storage.mocks.SlowDelaySuppliers}.
     */
    SINUSOIDAL_DISTRIBUTION_TYPE,
}
