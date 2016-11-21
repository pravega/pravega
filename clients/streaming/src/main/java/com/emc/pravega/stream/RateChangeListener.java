/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.stream;

/**
 * A listener that is notified every time a consumer may want to scale up or down the number of instances.
 */
public interface RateChangeListener {

    /**
     * Called when the rate changes, and scaling may be needed.
     *
     * @param stream            The stream that may need to be scaled.
     * @param isRebalanceUrgent True if a consumer may be blocked because
     * {@link RebalancerUtils#rebalance(java.util.Collection, int)} needs to be called.
     */
    void rateChanged(Stream stream, boolean isRebalanceUrgent);
}
