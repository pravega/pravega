package io.pravega.controller.server.bucket;

/**
 * Copyright Pravega Authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.Map;
import java.util.Set;

/**
 * Bucket Distributor are used to assign bucket among available controllers.
 */
public interface BucketDistributor {
    /**
     * Distribute the buckets among available controllers.
     *
     * @param previousMapping    Existing controllers to bucket mapping.
     * @param currentControllers Updated list of controllers in cluster.
     * @param bucketCount        Bucket count.
     * @return                   The new controller to buckets mapping after performing a distribute operation.
     */
    Map<String, Set<Integer>> distribute(Map<String, Set<Integer>> previousMapping, Set<String> currentControllers,
                                         int bucketCount);
}
