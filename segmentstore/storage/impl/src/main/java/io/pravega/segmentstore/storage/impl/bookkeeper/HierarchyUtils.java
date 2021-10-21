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
package io.pravega.segmentstore.storage.impl.bookkeeper;

/**
 * Utility methods for building a hierarchical node structure.
 */
final class HierarchyUtils {
    /**
     * Node path separator.
     */
    private static final String SEPARATOR = "/";

    /**
     * Used for extracting intermediate values for the path.
     */
    private static final int DIVISOR = 10;

    /**
     * Gets a hierarchical path using the value of nodeId to construct the intermediate paths.
     * Examples:
     * * nodeId = 1234, depth = 3 -> /4/3/2/1234
     * * nodeId = 1234, depth = 0 -> /1234
     * * nodeId = 1234, depth = 6 -> /4/3/2/1/0/0/1234
     *
     * @param nodeId The node id to create the path for.
     * @param depth  The hierarchy depth (0 means flat).
     * @return The hierarchical path.
     */
    static String getPath(int nodeId, int depth) {
        StringBuilder pathBuilder = new StringBuilder();
        int value = nodeId;
        for (int i = 0; i < depth; i++) {
            int r = value % DIVISOR;
            value = value / DIVISOR;
            pathBuilder.append(SEPARATOR).append(r);
        }

        return pathBuilder.append(SEPARATOR).append(nodeId).toString();
    }
}
