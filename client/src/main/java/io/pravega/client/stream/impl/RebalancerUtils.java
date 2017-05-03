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
package io.pravega.client.stream.impl;

import io.pravega.client.ClientFactory;
import io.pravega.client.stream.Position;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.Serializer;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * A set of utilities for managing consumers.
 */
public interface RebalancerUtils {
    /**
     * Given a time stamp returns positions corresponding (roughly) to that point in the stream.
     *
     * @param stream The stream for which positions are desired.
     * @param time The unix time that positions should be returned for.
     * @param numberOfConsumers The desired number of position objects
     * @return A set of position objects that can be passed to
     *         {@link ClientFactory#createReader(String, Serializer, ReaderConfig, Position)}
     *         to create a new consumer at the requested time.
     */
    Collection<Position> getInitialPositions(Stream stream, long time, int numberOfConsumers);

    /**
     * Given the positions from some existing consumers divide or combine them into positions for newNumberOfConsumers
     * consumers.
     *
     * @param consumers            The existing consumers that need to be rebalanced.
     * @param newNumberOfConsumers The desired number of consumers.
     * @return A collection of newNumberOfConsumers Positions that rebalance the same subset of events from a stream
     * that the provided consumers used.
     */
    Collection<Position> rebalance(Collection<Position> consumers, int newNumberOfConsumers);

    /**
     * Similar to {@link #rebalance(Collection, int)} but "sticky" in that consumers can correspond so some consistent
     * identifier. (Like a host name) And an explicit mapping will be provided.
     *
     * @param consumers    The existing consumers that need to be rebalanced.
     * @param newConsumers The new consumers.
     * @return A collection of newNumberOfConsumers Positions that rebalance the same subset of events from a stream
     * that the provided consumers used.
     */
    Map<String, Position> rebalance(Map<String, Position> consumers, List<String> newConsumers);
}
